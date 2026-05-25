Blueprints and Toolboxes
************************

Duct 1.2 introduces two new configuration structures in an effort to simplify
defining recurring patterns of monitoring checks. This is particularly useful
when monitoring a large list of hosts from a central point.

It's important to point out however that Duct is not well optimised for that
use case since each check permutation requires a resident timer object and
staggered startup can take a long time. To tune the stagger interval set 
the `stagger` global config variable to something very small, specifically
`acceptable_start_time/number_of_checks` in seconds. 

eg. If you have 1000 checks and the acceptable startup time is 5 add `stagger: 0.005`
to the configuration file.

Toolboxes
=========

A toolbox defines a set of sources without any unique options. It can also define defaults
to use for every source within that toolbox. ::

    toolbox:
      linux_defaults:
        defaults:
          use_ssh: True
          interval: 10.0
        sources:
          - service: memory
            source: duct.sources.linux.basic.Memory
          - service: cpu
            source: duct.sources.linux.basic.CPU

Blueprints
==========

A blueprint defines a set of lists to permute against specific options on a
toolbox.

If for example one wanted to apply the basic set of Linux checks specified
above to 3 hosts then the following configuration can be used. ::

    blueprint:
      - toolbox: linux_defaults
        defaults:
          interval: 10.0
        sets:
          hostname:
            - www01.acme.com
            - www02.acme.com
            - www03.acme.com

The equivalent long hand version of this configuration would be as follows ::

    sources:
        - service: memory
          source: duct.sources.linux.basic.Memory
          hostname: www01.acme.com
          use_ssh: True
          interval: 10.0

        - service: cpu
          source: duct.sources.linux.basic.CPU
          hostname: www01.acme.com
          use_ssh: True
          interval: 10.0

        - service: memory
          source: duct.sources.linux.basic.Memory
          hostname: www02.acme.com
          use_ssh: True
          interval: 10.0

        - service: cpu
          source: duct.sources.linux.basic.CPU
          hostname: www02.acme.com
          use_ssh: True
          interval: 10.0

        - service: memory
          source: duct.sources.linux.basic.Memory
          hostname: www03.acme.com
          use_ssh: True
          interval: 10.0

        - service: cpu
          source: duct.sources.linux.basic.CPU
          hostname: www03.acme.com
          use_ssh: True
          interval: 10.0

It's easy to see that the toolbox and blueprint pattern can considerably simplify
configuration.

A blueprint can also override defaults for a toolbox. Note also that a source does not
validate whether or not options passed to it are recognised, this has pro's and con's.
Namely it is possible to have an option set which would only apply to certain sources
in a toolbox which recognise that option, however it might also be possible to
inadvertently set options with shared names in the wrong place so always be sure to know
what collisions are possible.
