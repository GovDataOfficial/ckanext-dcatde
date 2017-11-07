# -*- coding: utf8 -*-
from mock import Mock


class GetActionHelper(object):
    '''Common helper to mock toolkit.get_action.

    Create a subclass which sets the return_val_actions and
    side_effect_actions dicts. Both dicts use the action to be mocked
    as key. The values are either the return value (return_val) or a function
    reference to be called (side_effect)'''

    def __init__(self):
        self.return_val_actions = {}
        self.side_effect_actions = {}
        self._mocked_actions = {}

    def build_mocks(self):
        '''Creates the mock objects for the specified return_val and
        side_effect actions. Has to be called after one of these
        dicts has changed.'''
        self._mocked_actions = {}
        for action in self.return_val_actions:
            mck = Mock()
            mck.return_value = self.return_val_actions[action]
            self._mocked_actions[action] = mck

        for action in self.side_effect_actions:
            mck = Mock()
            mck.side_effect = self.side_effect_actions[action]
            self._mocked_actions[action] = mck

    def get_mock_for(self, action):
        '''Returns the mock for the given action, or None if nonexisting.'''
        return self._mocked_actions.get(action)

    def mock_get_action(self, action):
        '''Returns a mock for the action, with return value or side effect
        set if available.'''
        if action in self._mocked_actions:
            return self._mocked_actions[action]

        # use a default mock otherwise
        return Mock()
