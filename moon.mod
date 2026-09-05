// Learn more about moon.mod configuration:
// https://docs.moonbitlang.com/en/latest/toolchain/moon/module.html
//
// To add a dependency, run this command in your terminal:
//   moon add moonbitlang/x
//
// Or manually declare it in `import`, for example:
// import {
//   "moonbitlang/x@0.4.6",
// }

name = "moonweave/moonweave"

version = "0.1.0"

readme = "README.mbt.md"

repository = ""

license = "Apache-2.0"

keywords = [
  "distributed-systems",
  "dataflow",
  "causality",
  "stream-processing",
]

preferred_target = "native"

description = "A causal-frontier incremental dataflow engine"

import {
  "moonbitlang/async@0.20.5",
}
