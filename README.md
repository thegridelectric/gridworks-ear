# Gridworks Ear

[![Tests](https://github.com/thegridelectric/gridworks-ear/workflows/Tests/badge.svg)][tests]

[![pre-commit](https://img.shields.io/badge/pre--commit-enabled-brightgreen?logo=pre-commit&logoColor=white)][pre-commit]
[![Black](https://img.shields.io/badge/code%20style-black-000000.svg)][black]

[tests]: https://github.com/thegridelectric/gridworks-ear/actions?workflow=Tests
[pre-commit]: https://github.com/pre-commit/pre-commit
[black]: https://github.com/psf/black

The GridWorks ear is responsible for putting all messages on its production world broker into persistent store.


1. Log into the instance 
  -  uses `gridworks-main.pem`
  - `ssh ubuntu@hw1-1-s3-ear.electricity.works`
2. ear service --help
 - ear service status

Also, there is an `ear` tmux running tail -n 500 -f state.txt in

~/.local/state/gridworks/ear/log

## Configurable exchange — running a scoped second instance

The exchange the ear's queue binds (`#`) is configuration:
`EAR_CONSUME_EXCHANGE`, default `ear_tx` (the universal audit tap — the
production ear needs no change). Pointing a **second instance** at a scoped
exchange captures a small, precious stream into its own store with the same
proven code. The first such instance is the **seed ear**: the Grid Node
Registry's slice (`gnr_ear_tx` — everything said to and by the registry:
create/re-parent commands, forest broadcasts, and the ack/nack write
verdicts, refusals included), written to the `gw-seedstore` bucket so the
fleet's topology record is findable at a glance rather than buried in the
telemetry torrent.

A second instance = a second `.env` + a second systemd unit:

```
EAR_CONSUME_EXCHANGE=gnr_ear_tx
EAR_WORLD_INSTANCE_ALIAS=hw1__1
EAR_AWS__BUCKET_NAME=gw-seedstore     # (the AwsClient bucket field)
```

with the service otherwise configured like the main ear. Same key grammar,
same S3 layout — readers swap only the bucket name.

## Contributing

For development, you will need a local dev rabbit broker. Set that up by downloading the gridworks-base repo and following the instructions [here](https://github.com/thegridelectric/gridworks-base?tab=readme-ov-file#dev-rabbit-broker) in its Readme.

Contributions are very welcome.
To learn more, see the [Contributor Guide].

## License

Distributed under the terms of the [MIT license][license],
_Gridworks Ear_ is free and open source software.

## Issues

If you encounter any problems,
please [file an issue] along with a detailed description.

## Credits

This project was generated from [@cjolowicz]'s [Hypermodern Python Cookiecutter] template.

[@cjolowicz]: https://github.com/cjolowicz
[pypi]: https://pypi.org/
[hypermodern python cookiecutter]: https://github.com/cjolowicz/cookiecutter-hypermodern-python
[file an issue]: https://github.com/thegridelectric/gridworks-ear/issues
[pip]: https://pip.pypa.io/

<!-- github-only -->

[license]: https://github.com/thegridelectric/gridworks-ear/blob/main/LICENSE
[contributor guide]: https://github.com/thegridelectric/gridworks-ear/blob/main/CONTRIBUTING.md
[command-line reference]: https://gridworks-ear.readthedocs.io/en/latest/usage.html
