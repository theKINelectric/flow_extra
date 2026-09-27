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
    package = Flowex.Mixfile.project() |> Keyword.fetch!(:package)
    files = package[:files]

    assert ".formatter.exs" in files
    assert "LICENSE" in files
    assert "figures" in files

    # Everything allowlisted exists in the checkout.
    for entry <- files do
      assert File.exists?(entry), "allowlisted entry missing: #{entry}"
    end
  end

  test "license metadata matches the checked-in notice (FX-010, decided)" do
    # The decision, 2026-09-27: Apache-2.0 — upstream's own LICENSE file
    # (Anton Mishchuk, 2017), verified present at the fork point and at
    # upstream master. The inherited "MIT" hex metadata contradicted the
    # author's own file and is not carried forward.
    package = Flowex.Mixfile.project() |> Keyword.fetch!(:package)

    assert package[:licenses] == ["Apache-2.0"]
    assert File.read!("LICENSE") =~ "Apache License, Version 2.0"
    assert File.read!("LICENSE") =~ "Copyright 2017 Anton Mishchuk"
  end
end
