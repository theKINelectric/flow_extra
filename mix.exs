defmodule Flowex.Mixfile do
  use Mix.Project

  def project do
    [
      app: :flowex,
      version: "0.5.4",
      # Honest floor: every dep in every env resolves on 1.15+ (ex_doc is the
      # binding constraint; gen_stage needs ~> 1.11, credo >= 1.13).
      elixir: "~> 1.15",
      start_permanent: Mix.env() == :prod,
      elixirc_paths: elixirc_paths(Mix.env()),
      deps: deps(),
      description: description(),
      package: package(),
      source_url: "https://github.com/antonmi/flowex"
    ]
  end

  def application do
    [
      mod: {Flowex.Application, []},
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
      files: ~w(lib mix.exs README.md),
      maintainers: ["Anton Mishchuk"],
      licenses: ["MIT"],
      links: %{"github" => "https://github.com/antonmi/flowex"}
    ]
  end
end
