# Transfer completion and bounded metadata

Moving jobs between workspaces (`httk job transfer`, `httk job eject`,
`httk job adopt` and the `httk transfer` operator verbs) is being rebuilt on the
filesystem kernel in this development version. Those commands keep their help
and refuse with exit status 2 until the rebuilt transfer layer lands; this page
returns with it.
