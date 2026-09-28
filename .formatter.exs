# The DSL's parenthesis-free form, for flowextra and for every project that
# formats with `import_deps: [:flowextra]`.
locals_without_parens = [pipe: 1, pipe: 2, error_pipe: 1, error_pipe: 2]

[
  inputs: ["{mix,.formatter}.exs", "{config,lib,test}/**/*.{ex,exs}"],
  locals_without_parens: locals_without_parens,
  export: [locals_without_parens: locals_without_parens]
]
