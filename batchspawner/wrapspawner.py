# Copyright (c) Regents of the University of Minnesota
# Distributed under the terms of the Modified BSD License.

"""WrapSpawner classes

WrapSpawner provides a mechanism to wrap the interface of a Spawner such that
the Spawner class to use for single-user servers can be chosen dynamically.
The child Spawner is created and started using the same logic as in User.spawn(),
but not until start() or load_state() is called. Thus, subclasses may modify the
class or properties of the child Spawner at any earlier point (e.g. from
Authenticator pre_spawn hooks or options form processing).

Note that there is no straightforward way to simply save the child_class Type
value in the ORM, so a subclass that provides a way to change the child_class
must also arrange to reproduce that change when state is reloaded.

Provided as an initial application is ProfilesSpawner, which accepts a list
of provided Spawner configurations, and generates an options form from that list.
Using this mechanism, the administrator can provide users with a pre-approved
selection of Spawner configurations.
"""
import os

from tornado import gen, concurrent

from jupyterhub.spawner import LocalProcessSpawner, Spawner
from traitlets import (
    Instance, Type, Tuple, List, Dict, Integer, Unicode, Float, Any
)

# Utility to create dummy Futures to return values through yields
def _yield_val(x=None):
    f = concurrent.Future()
    f.set_result(x)
    return f

class WrapSpawner(Spawner):

    # Grab this from constructor args in case some Spawner ever wants it
    config = Any()

    child_class = Type(LocalProcessSpawner, Spawner,
        config=True,
        help="""The class to wrap for spawning single-user servers.
                Should be a subclass of Spawner.
                """
        )

    child_config = Dict(default_value={},
        config=True,
        help="Dictionary of config values to apply to wrapped spawner class."
        )

    child_state = Dict(default_value={})

    child_spawner = Instance(Spawner, allow_none=True)

    def construct_child(self):
        if self.child_spawner is None:
            self.child_spawner = self.child_class(
                user = self.user,
                db   = self.db,
                hub  = self.hub,
                authenticator = self.authenticator,
                config = self.config,
                **self.child_config
                )
            # initial state will always be wrong since it will see *our* state
            self.child_spawner.clear_state()
            if self.child_state:
                self.child_spawner.load_state(self.child_state)
            self.child_spawner.api_token = self.api_token
        return self.child_spawner

    def load_child_class(self, state):
        # Subclasses must arrange for correct child_class setting from load_state
        pass

    def load_state(self, state):
        super().load_state(state)
        self.load_child_class(state)
        self.child_config.update(state.get('child_conf', {}))
        self.child_state = state.get('child_state', {})
        self.construct_child()

    def get_state(self):
        state = super().get_state()
        state['child_conf'] = self.child_config
        if self.child_spawner:
            self.child_state = state['child_state'] = self.child_spawner.get_state()
        return state

    def clear_state(self):
        super().clear_state()
        if self.child_spawner:
            self.child_spawner.clear_state()
        self.child_state = {}
        self.child_spawner = None

    # proxy functions for start/poll/stop
    # pass back the child's Future, or create a dummy if needed

    def start(self):
        if not self.child_spawner:
            self.construct_child()
        return self.child_spawner.start()

    def stop(self, now=False):
        if self.child_spawner:
            return self.child_spawner.stop(now)
        else:
            return _yield_val()

    def poll(self):
        if self.child_spawner:
            return self.child_spawner.poll()
        else:
            return _yield_val(1)

class ProfilesSpawner(WrapSpawner):

    """ProfilesSpawner - leverages the Spawner options form feature to allow user-driven
        configuration of Spawner classes while permitting:
        1) configuration of Spawner classes that don't natively implement options_form
        2) administrator control of allowed configuration changes
        3) runtime choice of which Spawner backend to launch
    """

    profiles = List(
        trait = Tuple( Unicode(), Unicode(), Type(Spawner), Dict() ),
        default_value = [ ( 'Local Notebook Server', 'local', LocalProcessSpawner, 
                            {'start_timeout': 15, 'http_timeout': 10} ) ],
        minlen = 1,
        config = True,
        help = """List of profiles to offer for selection. Signature is:
            List(Tuple( Unicode, Unicode, Type(Spawner), Dict )) corresponding to
            profile display name, unique key, Spawner class, dictionary of spawner config options.

            The first three values will be exposed in the input_template as {display}, {key}, and {type}"""
        )

    child_profile = Unicode()

    form_template = Unicode(
        """<label for="profile">Select a job profile:</label>
        <select class="form-control" name="profile" required autofocus>
        {input_template}
        </select>
        """,
        config = True,
        help = """Template to use to construct options_form text. {input_template} is replaced with
            the result of formatting input_template against each item in the profiles list."""
        )

    first_template = Unicode('selected',
        config=True,
        help="Text to substitute as {first} in input_template"
        )

    input_template = Unicode("""
        <option value="{key}" {first}>{display}</option>""",
        config = True,
        help = """Template to construct {input_template} in form_template. This text will be formatted
            against each item in the profiles list, in order, using the following key names:
            ( display, key, type ) for the first three items in the tuple, and additionally
            first = "checked" (taken from first_template) for the first item in the list, so that
            the first item starts selected."""
        )

    options_form = Unicode()

    def _options_form_default(self):
        temp_keys = [ dict(display=p[0], key=p[1], type=p[2], first='') for p in self.profiles ]
        temp_keys[0]['first'] = self.first_template
        text = ''.join([ self.input_template.format(**tk) for tk in temp_keys ])
        return self.form_template.format(input_template=text)

    def options_from_form(self, formdata):
        # Default to first profile if somehow none is provided
        return dict(profile=formdata.get('profile', [self.profiles[0][1]])[0])

    # load/get/clear : save/restore child_profile (and on load, use it to update child class/config)

    def select_profile(self, profile):
        # Select matching profile, or do nothing (leaving previous or default config in place)
        for p in self.profiles:
            if p[1] == profile:
                self.child_class = p[2]
                self.child_config = p[3]
                break

    def construct_child(self):
        self.child_profile = self.user_options.get('profile', "")
        self.select_profile(self.child_profile)
        super().construct_child()

    def load_child_class(self, state):
        self.child_profile = state['profile']
        self.select_profile(self.child_profile)

    def get_state(self):
        state = super().get_state()
        state['profile'] = self.child_profile
        return state

    def clear_state(self):
        super().clear_state()
        self.child_profile = ''

# vim: set ai expandtab softtabstop=4:

