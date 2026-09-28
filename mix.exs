defmodule FlowExtra.Mixfile do
  use Mix.Project

  def project do
    [
      app: :flowextra,
      # 0.6.0: the fork's own line — admission protocol, one-deadline calls,
      # supervised stop semantics; cast/2's refusal return is breaking.
      version: "0.6.0",
      # Honest floor: every dep in every env resolves on 1.15+ (ex_doc is the
      # binding constraint; gen_stage needs ~> 1.11, credo >= 1.13).
      elixir: "~> 1.15",
      start_permanent: Mix.env() == :prod,
      elixirc_paths: elixirc_paths(Mix.env()),
      deps: deps(),
      description: description(),
      package: package(),
      source_url: "https://codeberg.org/kin_electric/flowex"
    ]
  end

  def application do
    [
      mod: {FlowExtra.Application, []},
      extra_applications: []
    ]
  end

  defp elixirc_paths(:test), do: ["lib", "test/support"]
  defp elixirc_paths(_), do: ["lib"]

  defp deps do
    [
      {:gen_stage, "~> 1.0"},
      {:credo, "~> 1.4", only: [:dev, :test]},
      {:dialyxir, "~> 1.4", only: [:dev], runtime: false},
      # Docs
      {:ex_doc, "~> 0.22", only: [:docs, :dev]}
    ]
  end

  defp description do
    "Flow-Based Programming with Elixir GenStage."
  end

  defp package do
    [
      # The artifact must carry what consumers need downstream (FX-009): the
      # formatter export (import_deps: [:flowextra]), the complete Apache-2.0
      # terms with preserved attribution, the migration guide from Flowex,
      # and the README's figures. Apache-2.0 is upstream's own license (Anton
      # Mishchuk, 2017 — d78a761, tagged v0.5.1), verified present at the
      # fork point and at upstream master; the inherited "MIT" hex metadata
      # contradicted the author's own file and is not carried forward.
      files: ~w(lib mix.exs README.md MIGRATION.md .formatter.exs LICENSE figures),
      maintainers: ["Anton Mishchuk", "kin_electric"],
      licenses: ["Apache-2.0"],
      links: %{
        "Codeberg" => "https://codeberg.org/kin_electric/flowex",
        "GitHub (upstream)" => "https://github.com/antonmi/flowex"
      }
    ]
  end
end
