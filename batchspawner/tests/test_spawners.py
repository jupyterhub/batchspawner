"""Test BatchSpawner and subclasses"""

import asyncio
import pwd
import re
import time
from getpass import getuser
from unittest import mock

import pytest
from jupyterhub import orm
from jupyterhub.objects import Hub, Server
from jupyterhub.user import User
from traitlets import Unicode

from .. import BatchSpawnerRegexStates, JobStatus

testhost = "userhost123"
testjob = "12345"
testport = 54321


@pytest.fixture(autouse=True)
def _always_get_my_home():
    # pwd.getbwnam() is always called with the current user
    # ignoring the requested name, which usually doesn't exist
    getpwnam = pwd.getpwnam
    with mock.patch.object(pwd, "getpwnam", lambda name: getpwnam(getuser())):
        yield


class BatchDummy(BatchSpawnerRegexStates):
    exec_prefix = ""
    batch_submit_cmd = Unicode("cat > /dev/null; echo " + testjob)
    batch_query_cmd = Unicode("echo RUN " + testhost)
    batch_cancel_cmd = Unicode("echo STOP")
    batch_script = Unicode("{cmd}")
    state_pending_re = Unicode("PEND")
    state_running_re = Unicode("RUN")
    state_exechost_re = Unicode("RUN (.*)$")
    state_unknown_re = Unicode("UNKNOWN")

    cmd_expectlist = None
    out_expectlist = None

    async def run_command(self, *args, **kwargs):
        """Overwriten run command to test templating and outputs"""
        cmd = args[0]
        # Test that the command matches the expectations
        if self.cmd_expectlist:
            run_re = self.cmd_expectlist.pop(0)
            if run_re:
                print("run:", run_re)
                assert (
                    run_re.search(cmd) is not None
                ), f"Failed test: re={run_re} cmd={cmd}"
        # Run command normally
        out = await super().run_command(*args, **kwargs)
        # Test that the command matches the expectations
        if self.out_expectlist:
            out_re = self.out_expectlist.pop(0)
            if out_re:
                print("out:", out_re)
                assert (
                    out_re.search(cmd) is not None
                ), f"Failed output: re={out_re} cmd={cmd} out={out}"
        return out


def new_spawner(db, spawner_class=BatchDummy, **kwargs):
    kwargs.setdefault("cmd", ["singleuser_command"])
    user = db.query(orm.User).first()
    hub = Hub()
    user = User(user, {})
    server = Server()
    # Set it after constructions because it isn't a traitlet.
    kwargs.setdefault("hub", hub)
    kwargs.setdefault("user", user)
    kwargs.setdefault("poll_interval", 1)

    # These are not traitlets so we have to set them here
    spawner = user._new_spawner("", spawner_class=spawner_class, **kwargs)
    spawner.server = server
    spawner.mock_port = testport
    return spawner


def check_ip(spawner, value):
    assert spawner.ip == value


async def test_spawner_start_stop_poll(db, event_loop):
    spawner = new_spawner(db=db)

    status = await asyncio.wait_for(spawner.poll(), timeout=5)
    assert status == 1
    assert spawner.job_id == ""
    assert spawner.get_state() == {}

    await asyncio.wait_for(spawner.start(), timeout=5)
    check_ip(spawner, testhost)
    assert spawner.job_id == testjob

    status = await asyncio.wait_for(spawner.poll(), timeout=5)
    assert status is None
    spawner.batch_query_cmd = "echo NOPE"
    await asyncio.wait_for(spawner.stop(), timeout=5)
    status = await asyncio.wait_for(spawner.poll(), timeout=5)
    assert status == 1
    assert spawner.get_state() == {}


async def test_stress_submit(db, event_loop):
    for i in range(200):
        time.sleep(0.01)
        test_spawner_start_stop_poll(db, event_loop)


async def test_spawner_state_reload(db, event_loop):
    spawner = new_spawner(db=db)
    assert spawner.get_state() == {}

    await asyncio.wait_for(spawner.start(), timeout=30)
    check_ip(spawner, testhost)
    assert spawner.job_id == testjob

    state = spawner.get_state()
    assert state == dict(job_id=testjob, job_status="RUN " + testhost)
    spawner = new_spawner(db=db)
    spawner.clear_state()
    assert spawner.get_state() == {}
    spawner.load_state(state)
    # We used to check IP here, but that is actually only computed on start(),
    # and is not part of the spawner's persistent state
    assert spawner.job_id == testjob


async def test_submit_failure(db, event_loop):
    spawner = new_spawner(db=db)
    assert spawner.get_state() == {}
    spawner.batch_submit_cmd = "cat > /dev/null; true"
    with pytest.raises(RuntimeError):
        await asyncio.wait_for(spawner.start(), timeout=30)
    assert spawner.job_id == ""
    assert spawner.job_status == ""


async def test_submit_pending_fails(db, event_loop):
    """Submission works, but the batch query command immediately fails"""
    spawner = new_spawner(db=db)
    assert spawner.get_state() == {}
    spawner.batch_query_cmd = "echo xyz"
    with pytest.raises(RuntimeError):
        await asyncio.wait_for(spawner.start(), timeout=30)
    status = await asyncio.wait_for(spawner.query_job_status(), timeout=30)
    assert status == JobStatus.NOTFOUND
    assert spawner.job_id == ""
    assert spawner.job_status == ""


async def test_poll_fails(db, event_loop):
    """Submission works, but a later .poll() fails"""
    spawner = new_spawner(db=db)
    assert spawner.get_state() == {}
    # The start is successful:
    await asyncio.wait_for(spawner.start(), timeout=30)
    spawner.batch_query_cmd = "echo xyz"
    # Now, the poll fails:
    await asyncio.wait_for(spawner.poll(), timeout=30)
    # .poll() will run self.clear_state() if it's not found:
    assert spawner.job_id == ""
    assert spawner.job_status == ""


async def test_unknown_status(db, event_loop):
    """Polling returns an unknown status"""
    spawner = new_spawner(db=db)
    assert spawner.get_state() == {}
    # The start is successful:
    await asyncio.wait_for(spawner.start(), timeout=30)
    spawner.batch_query_cmd = "echo UNKNOWN"
    # This poll should not fail:
    await asyncio.wait_for(spawner.poll(), timeout=30)
    status = await asyncio.wait_for(spawner.query_job_status(), timeout=30)
    assert status == JobStatus.UNKNOWN
    assert spawner.job_id == "12345"
    assert spawner.job_status != ""


async def test_templates(db, event_loop):
    """Test templates in the run_command commands"""
    spawner = new_spawner(db=db)

    # Test when not running
    spawner.cmd_expectlist = [
        re.compile(".*RUN"),
    ]
    status = await asyncio.wait_for(spawner.poll(), timeout=5)
    assert status == 1
    assert spawner.job_id == ""
    assert spawner.get_state() == {}

    # Test starting
    spawner.cmd_expectlist = [
        re.compile(".*echo"),
        re.compile(".*RUN"),
    ]
    await asyncio.wait_for(spawner.start(), timeout=5)
    check_ip(spawner, testhost)
    assert spawner.job_id == testjob

    # Test poll - running
    spawner.cmd_expectlist = [
        re.compile(".*RUN"),
    ]
    status = await asyncio.wait_for(spawner.poll(), timeout=5)
    assert status is None

    # Test stopping
    spawner.batch_query_cmd = "echo NOPE"
    spawner.cmd_expectlist = [
        re.compile(".*STOP"),
        re.compile(".*NOPE"),
    ]
    await asyncio.wait_for(spawner.stop(), timeout=5)
    status = await asyncio.wait_for(spawner.poll(), timeout=5)
    assert status == 1
    assert spawner.get_state() == {}


async def test_batch_script(db, event_loop):
    """Test that the batch script substitutes {cmd}"""

    class BatchDummyTestScript(BatchDummy):
        async def _get_batch_script(self, **subvars):
            script = await super()._get_batch_script(**subvars)
            assert "singleuser_command" in script
            return script

    spawner = new_spawner(db=db, spawner_class=BatchDummyTestScript)
    # status = await asyncio.wait_for(spawner.poll(), timeout=5)
    await asyncio.wait_for(spawner.start(), timeout=5)
    # status = await asyncio.wait_for(spawner.poll(), timeout=5)
    # await asyncio.wait_for(spawner.stop(), timeout=5)


async def test_exec_prefix(db, event_loop):
    """Test that all run_commands have exec_prefix"""

    class BatchDummyTestScript(BatchDummy):
        exec_prefix = "PREFIX"

        async def run_command(self, cmd, *args, **kwargs):
            assert cmd.startswith("PREFIX ")
            cmd = cmd[7:]
            print(cmd)
            out = await super().run_command(cmd, *args, **kwargs)
            return out

    spawner = new_spawner(db=db, spawner_class=BatchDummyTestScript)
    # Not running
    status = await asyncio.wait_for(spawner.poll(), timeout=5)
    assert status == 1
    # Start
    await asyncio.wait_for(spawner.start(), timeout=5)
    assert spawner.job_id == testjob
    # Poll
    status = await asyncio.wait_for(spawner.poll(), timeout=5)
    assert status is None
    # Stop
    spawner.batch_query_cmd = "echo NOPE"
    await asyncio.wait_for(spawner.stop(), timeout=5)
    status = await asyncio.wait_for(spawner.poll(), timeout=5)
    assert status == 1


async def run_spawner_script(
    db, spawner, script, batch_script_re_list=None, spawner_kwargs={}
):
    """Run a spawner script and test that the output and behavior is as expected.

    db: same as in this module
    spawner: the BatchSpawnerBase subclass to test
    script: list of (input_re_to_match, output)
    batch_script_re_list: if given, assert batch script matches all of these
    """
    # Create the expected scripts
    cmd_expectlist, out_list = zip(*script)
    cmd_expectlist = list(cmd_expectlist)
    out_list = list(out_list)

    class BatchDummyTestScript(spawner):
        async def run_command(self, cmd, input=None, env=None):
            # Test the input
            run_re = cmd_expectlist.pop(0)
            if run_re:
                print(f'run: "{cmd}"   [{run_re}]')
                assert (
                    run_re.search(cmd) is not None
                ), f"Failed test: re={run_re} cmd={cmd}"
            # Test the stdin - will only be the batch script.  For
            # each regular expression in batch_script_re_list, assert that
            # each re in that list matches the batch script.
            if batch_script_re_list and input:
                batch_script = input
                for match_re in batch_script_re_list:
                    assert (
                        match_re.search(batch_script) is not None
                    ), f"Batch script does not match {match_re}"
            # Return expected output.
            out = out_list.pop(0)
            print("  --> " + out)
            return out

    spawner = new_spawner(db=db, spawner_class=BatchDummyTestScript, **spawner_kwargs)
    # Not running at beginning (no command run)
    status = await asyncio.wait_for(spawner.poll(), timeout=5)
    assert status == 1
    # batch_submit_cmd
    # batch_query_cmd    (result=pending)
    # batch_query_cmd    (result=running)
    await asyncio.wait_for(spawner.start(), timeout=5)
    assert spawner.job_id == testjob
    check_ip(spawner, testhost)
    # batch_query_cmd
    status = await asyncio.wait_for(spawner.poll(), timeout=5)
    assert status is None
    # batch_cancel_cmd
    await asyncio.wait_for(spawner.stop(), timeout=5)
    # batch_poll_cmd
    status = await asyncio.wait_for(spawner.poll(), timeout=5)
    assert status == 1


async def test_torque(db, event_loop):
    spawner_kwargs = {
        "req_nprocs": "5",
        "req_memory": "5678",
        "req_options": "some_option_asdf",
        "req_prologue": "PROLOGUE",
        "req_epilogue": "EPILOGUE",
    }
    batch_script_re_list = [
        re.compile(
            r"^PROLOGUE.*^batchspawner-singleuser singleuser_command.*^EPILOGUE",
            re.S | re.M,
        ),
        re.compile(r"mem=5678"),
        re.compile(r"ppn=5"),
        re.compile(r"^#PBS some_option_asdf", re.M),
    ]
    script = [
        (re.compile(r"sudo.*qsub"), str(testjob)),
        (
            re.compile(r"sudo.*qstat"),
            "<job_state>Q</job_state><exec_host></exec_host>",
        ),  # pending
        (
            re.compile(r"sudo.*qstat"),
            f"<job_state>R</job_state><exec_host>{testhost}/1</exec_host>",
        ),  # running
        (
            re.compile(r"sudo.*qstat"),
            f"<job_state>R</job_state><exec_host>{testhost}/1</exec_host>",
        ),  # running
        (re.compile(r"sudo.*qdel"), "STOP"),
        (re.compile(r"sudo.*qstat"), ""),
    ]
    from .. import TorqueSpawner

    await run_spawner_script(
        db,
        TorqueSpawner,
        script,
        batch_script_re_list=batch_script_re_list,
        spawner_kwargs=spawner_kwargs,
    )


async def test_moab(db, event_loop):
    spawner_kwargs = {
        "req_nprocs": "5",
        "req_memory": "5678",
        "req_options": "some_option_asdf",
        "req_prologue": "PROLOGUE",
        "req_epilogue": "EPILOGUE",
    }
    batch_script_re_list = [
        re.compile(
            r"^PROLOGUE.*^batchspawner-singleuser singleuser_command.*^EPILOGUE",
            re.S | re.M,
        ),
        re.compile(r"mem=5678"),
        re.compile(r"ppn=5"),
        re.compile(r"^#PBS some_option_asdf", re.M),
    ]
    script = [
        (re.compile(r"sudo.*msub"), str(testjob)),
        (re.compile(r"sudo.*mdiag"), 'State="Idle"'),  # pending
        (
            re.compile(r"sudo.*mdiag"),
            f'State="Running" AllocNodeList="{testhost}"',
        ),  # running
        (
            re.compile(r"sudo.*mdiag"),
            f'State="Running" AllocNodeList="{testhost}"',
        ),  # running
        (re.compile(r"sudo.*mjobctl.*-c"), "STOP"),
        (re.compile(r"sudo.*mdiag"), ""),
    ]
    from .. import MoabSpawner

    await run_spawner_script(
        db,
        MoabSpawner,
        script,
        batch_script_re_list=batch_script_re_list,
        spawner_kwargs=spawner_kwargs,
    )


async def test_pbs(db, event_loop):
    spawner_kwargs = {
        "req_nprocs": "4",
        "req_memory": "10256",
        "req_options": "some_option_asdf",
        "req_host": "some_pbs_admin_node",
        "req_runtime": "08:00:00",
    }
    batch_script_re_list = [
        re.compile(r"singleuser_command"),
        re.compile(r"select=1"),
        re.compile(r"ncpus=4"),
        re.compile(r"mem=10256"),
        re.compile(r"walltime=08:00:00"),
        re.compile(r"@some_pbs_admin_node"),
        re.compile(r"^#PBS some_option_asdf", re.M),
    ]
    script = [
        (re.compile(r"sudo.*qsub"), str(testjob)),
        (re.compile(r"sudo.*qstat"), "job_state = Q"),  # pending
        (
            re.compile(r"sudo.*qstat"),
            f"job_state = R\nexec_host = {testhost}/2*1",
        ),  # running
        (
            re.compile(r"sudo.*qstat"),
            f"job_state = R\nexec_host = {testhost}/2*1",
        ),  # running
        (re.compile(r"sudo.*qdel"), "STOP"),
        (re.compile(r"sudo.*qstat"), ""),
    ]
    from .. import PBSSpawner

    await run_spawner_script(
        db,
        PBSSpawner,
        script,
        batch_script_re_list=batch_script_re_list,
        spawner_kwargs=spawner_kwargs,
    )


async def test_slurm(db, event_loop):
    spawner_kwargs = {
        "req_runtime": "3-05:10:10",
        "req_nprocs": "5",
        "req_memory": "5678",
        "req_options": "some_option_asdf",
        "req_prologue": "PROLOGUE",
        "req_epilogue": "EPILOGUE",
        "req_reservation": "RES123",
        "req_gres": "GRES123",
    }
    batch_script_re_list = [
        re.compile(
            r"PROLOGUE.*srun batchspawner-singleuser singleuser_command.*EPILOGUE", re.S
        ),
        re.compile(r"^#SBATCH \s+ --cpus-per-task=5", re.X | re.M),
        re.compile(r"^#SBATCH \s+ --time=3-05:10:10", re.X | re.M),
        re.compile(r"^#SBATCH \s+ some_option_asdf", re.X | re.M),
        re.compile(r"^#SBATCH \s+ --reservation=RES123", re.X | re.M),
        re.compile(r"^#SBATCH \s+ --gres=GRES123", re.X | re.M),
    ]
    from .. import SlurmSpawner

    await run_spawner_script(
        db,
        SlurmSpawner,
        normal_slurm_script,
        batch_script_re_list=batch_script_re_list,
        spawner_kwargs=spawner_kwargs,
    )


# We tend to use slurm as our typical example job.  These allow quick
# Slurm examples.
normal_slurm_script = [
    (re.compile(r"sudo.*sbatch"), str(testjob)),
    (re.compile(r"sudo.*squeue"), "PENDING "),  # pending
    (
        re.compile(r"sudo.*squeue"),
        "slurm_load_jobs error: Unable to contact slurm controller",
    ),  # unknown
    (re.compile(r"sudo.*squeue"), "RUNNING " + testhost),  # running
    (re.compile(r"sudo.*squeue"), "RUNNING " + testhost),
    (re.compile(r"sudo.*scancel"), "STOP"),
    (re.compile(r"sudo.*squeue"), ""),
]
from .. import SlurmSpawner


async def run_typical_slurm_spawner(
    db,
    spawner=SlurmSpawner,
    script=normal_slurm_script,
    batch_script_re_list=None,
    spawner_kwargs={},
):
    """Run a full slurm job with default (overrideable) parameters.

    This is useful, for example, for changing options and testing effect
    of batch scripts.
    """
    return await run_spawner_script(
        db,
        spawner,
        script,
        batch_script_re_list=batch_script_re_list,
        spawner_kwargs=spawner_kwargs,
    )


# async def test_gridengine(db, event_loop):
#    spawner_kwargs = {
#        'req_options': 'some_option_asdf',
#        }
#    batch_script_re_list = [
#        re.compile(r'singleuser_command'),
#        re.compile(r'#$\s+some_option_asdf'),
#        ]
#    script = [
#        (re.compile(r'sudo.*qsub'),   'x x '+str(testjob)),
#        (re.compile(r'sudo.*qstat'),   'PENDING '),
#        (re.compile(r'sudo.*qstat'),   'RUNNING '+testhost),
#        (re.compile(r'sudo.*qstat'),   'RUNNING '+testhost),
#        (re.compile(r'sudo.*qdel'),  'STOP'),
#        (re.compile(r'sudo.*qstat'),   ''),
#        ]
#    from .. import GridengineSpawner
#    await run_spawner_script(db, GridengineSpawner, script,
#                       batch_script_re_list=batch_script_re_list,
#                       spawner_kwargs=spawner_kwargs)


async def test_condor(db, event_loop):
    spawner_kwargs = {
        "req_nprocs": "5",
        "req_memory": "5678",
        "req_options": "some_option_asdf",
    }
    batch_script_re_list = [
        re.compile(r"exec batchspawner-singleuser singleuser_command"),
        re.compile(r"RequestCpus = 5"),
        re.compile(r"RequestMemory = 5678"),
        re.compile(r"^some_option_asdf", re.M),
    ]
    script = [
        (
            re.compile(r"sudo.*condor_submit"),
            f"submitted to cluster {str(testjob)}",
        ),
        (re.compile(r"sudo.*condor_q"), "1,"),  # pending
        (re.compile(r"sudo.*condor_q"), f"2, @{testhost}"),  # runing
        (re.compile(r"sudo.*condor_q"), f"2, @{testhost}"),
        (re.compile(r"sudo.*condor_rm"), "STOP"),
        (re.compile(r"sudo.*condor_q"), ""),
    ]
    from .. import CondorSpawner

    await run_spawner_script(
        db,
        CondorSpawner,
        script,
        batch_script_re_list=batch_script_re_list,
        spawner_kwargs=spawner_kwargs,
    )


async def test_lfs(db, event_loop):
    spawner_kwargs = {
        "req_nprocs": "5",
        "req_memory": "5678",
        "req_options": "some_option_asdf",
        "req_queue": "some_queue",
        "req_prologue": "PROLOGUE",
        "req_epilogue": "EPILOGUE",
    }
    batch_script_re_list = [
        re.compile(
            r"^PROLOGUE.*^batchspawner-singleuser singleuser_command.*^EPILOGUE",
            re.S | re.M,
        ),
        re.compile(r"#BSUB\s+-q\s+some_queue", re.M),
    ]
    script = [
        (
            re.compile(r"sudo.*bsub"),
            f"Job <{str(testjob)}> is submitted to default queue <normal>",
        ),
        (re.compile(r"sudo.*bjobs"), "PEND "),  # pending
        (re.compile(r"sudo.*bjobs"), f"RUN {testhost}"),  # running
        (re.compile(r"sudo.*bjobs"), f"RUN {testhost}"),
        (re.compile(r"sudo.*bkill"), "STOP"),
        (re.compile(r"sudo.*bjobs"), ""),
    ]
    from .. import LsfSpawner

    await run_spawner_script(
        db,
        LsfSpawner,
        script,
        batch_script_re_list=batch_script_re_list,
        spawner_kwargs=spawner_kwargs,
    )


async def test_keepvars(db, event_loop):
    # req_keepvars
    spawner_kwargs = {
        "req_keepvars": "ABCDE",
    }
    batch_script_re_list = [
        re.compile(r"--export=ABCDE", re.X | re.M),
    ]
    await run_typical_slurm_spawner(
        db,
        spawner_kwargs=spawner_kwargs,
        batch_script_re_list=batch_script_re_list,
    )

    # req_keepvars AND req_keepvars together
    spawner_kwargs = {
        "req_keepvars": "ABCDE",
        "req_keepvars_extra": "XYZ",
    }
    batch_script_re_list = [
        re.compile(r"--export=ABCDE,XYZ", re.X | re.M),
    ]
    await run_typical_slurm_spawner(
        db,
        spawner_kwargs=spawner_kwargs,
        batch_script_re_list=batch_script_re_list,
    )
