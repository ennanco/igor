#!/bin/sh
set -eu

cat > extractor-input.json
[ "$(cat stdout.log)" = 'training-output' ]

printf '%s\n' 'julia score: 0.875' > julia-metrics.txt

printf '%s\n' '{"schema_version":1,"status":"success","metrics":{"Julia score":0.875,"loss_final":0.125},"selection":{"split":"validation","metric":"Julia score","value":0.875},"artifacts":[{"path":"julia-metrics.txt","role":"metrics"}]}'
