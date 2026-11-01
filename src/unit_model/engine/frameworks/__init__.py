"""PHP Framework Lifecycle Adapters for FreeUnit Model A.

Contains state machine lifecycle models and response profiles for:
- WordPress
- Drupal
- Symfony
- Laravel Octane
- Yii3

The response sizes and write patterns are assumptions documented per adapter
(see profile.py); they are inputs to the simulation, not facts about FreeUnit.
"""

from unit_model.engine.frameworks.profile import PHP_OUTPUT_BUFFERING, ResponseProfile

__all__ = ["PHP_OUTPUT_BUFFERING", "ResponseProfile"]
