defmodule Flowex.PackageTest do
  use ExUnit.Case, async: true

  @moduledoc """
  FX-009 release gate: the package allowlist must carry what downstream
  consumers need — the formatter export (`import_deps: [:flowex]` reads it),
  the license notice, and the README's figures. The audit found the built
  artifact shipping only lib, mix.exs, and README.md, so the formatter-export
  feature tested in the checkout was absent from the distribution. The full
  artifact-level exercise lives in `package_downstream_test.exs`
  (`--only downstream`).
  """

  test "the package carries the formatter export, license notice, and figures" do
    files = Flowex.Mixfile.project() |> Keyword.fetch!(:package) |> Keyword.fetch!(:files)

    assert ".formatter.exs" in files
    assert "LICENSE" in files
    assert "figures" in files

    # Everything allowlisted exists in the checkout.
    for entry <- files do
      assert File.exists?(entry), "allowlisted entry missing: #{entry}"
    end
  end
end
