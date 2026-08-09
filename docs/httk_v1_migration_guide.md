# Migrating an *httk* v1 workflow to *httk₂*

*For maintainers of an httk v1 workflow, moving it to httk₂ at whatever pace
suits.* An existing `ht_steps`/`ht_run` workflow can be kept unchanged in a
converted package (`language = "httk-v1"`), migrated one job type at a time,
or rewritten against the native APIs; finished v1 trees are harvested with
`httk workflow v1 collect`. The central rule: migrate task definitions and
newly instantiated task directories, never a live v1 task-manager queue.

The full guide, {doc}`details/httk_v1_migration_guide`, walks through every
step; {doc}`v1_compatibility` describes the compatibility surface itself.
