defmodule Flowex.PackageDownstreamTest do
  use ExUnit.Case, async: false

  @moduledoc """
  FX-009 artifact-level verification, enforced on every suite run: build the
  real package, extract it, scaffold a fresh downstream project that depends
  on NOTHING but the extracted artifact, and prove the formatter export
  arrives (a consumer's `pipe :double, count: 2` keeps its form) and a
  pipeline actually runs (21 → 42). The pursuit E checklist is exercised in
  full from the artifact itself: startup, one successful pipeline, the
  error route (:boom recovered by the error pipe), and a deadline
  (PipelineError :timeout, not a hang). License metadata is pinned
  separately in `package_test.exs` (Apache-2.0, decided 2026-09-27 — see
  docs/research/flowex/E-artifact-and-provenance.md).
  """

  test "a fresh downstream project formats and runs from the built artifact" do
    repo = File.cwd!()
    tmp = Path.join(System.tmp_dir!(), "flowex_downstream_#{System.unique_integer([:positive])}")
    artifact = Path.join(tmp, "flowex.tar")
    extracted = Path.join(tmp, "artifact")

    File.mkdir_p!(extracted)

    try do
      {build_out, 0} =
        System.cmd("mix", ["hex.build", "--output", artifact],
          cd: repo,
          stderr_to_stdout: true,
          env: %{"MIX_ENV" => "dev"}
        )

      assert build_out =~ "Building", build_out

      {_, 0} =
        System.cmd(
          "sh",
          ["-c", "tar -xOf #{artifact} contents.tar.gz | tar -xzf - -C #{extracted}"],
          cd: tmp
        )

      # The artifact carries what the checkout promised (FX-009's original
      # omissions: formatter export, license, figures).
      assert File.exists?(Path.join(extracted, ".formatter.exs"))
      assert File.exists?(Path.join(extracted, "LICENSE"))
      assert File.exists?(Path.join(extracted, "figures/pipeline_with_client.png"))

      # A fresh downstream project, depending only on the extracted artifact.
      {_, 0} = System.cmd("mix", ["new", "downstream_app"], cd: tmp, stderr_to_stdout: true)

      downstream = Path.join(tmp, "downstream_app")
      mixfile = Path.join(downstream, "mix.exs")

      mixfile
      |> File.read!()
      |> String.replace(
        "defp deps do\n    [\n",
        "defp deps do\n    [\n      {:flowex, path: #{inspect(extracted)}}\n"
      )
      |> then(&File.write!(mixfile, &1))

      assert mixfile |> File.read!() =~ "{:flowex, path:"

      # Normalize the injection itself; the check below must judge the DSL
      # file's form, not this test's string surgery.
      {_, 0} = System.cmd("mix", ["format", "mix.exs"], cd: downstream, stderr_to_stdout: true)

      # The consumer's formatter imports flowex's export: the DSL keeps its
      # paren-less form only if the artifact really carries it.
      File.write!(Path.join(downstream, ".formatter.exs"), """
      [
        import_deps: [:flowex],
        inputs: ["{mix,.formatter}.exs", "{config,lib,test}/**/*.{ex,exs}"]
      ]
      """)

      File.mkdir_p!(Path.join(downstream, "lib"))

      File.write!(Path.join(downstream, "lib/consumer_pipeline.ex"), """
      defmodule ConsumerApp.Pipeline do
        use Flowex.Pipeline

        defstruct number: nil

        pipe :double, count: 2
        error_pipe :rescue_it

        def double(%{number: n}, _opts), do: %{number: n * 2}
        def rescue_it(_error, struct, _opts), do: struct
      end

      defmodule ConsumerApp.SlowPipeline do
        use Flowex.Pipeline

        defstruct number: nil

        pipe :slow

        def slow(%{number: n}, _opts) do
          Process.sleep(200)
          %{number: n}
        end
      end
      """)

      {_, 0} = System.cmd("mix", ["deps.get"], cd: downstream, stderr_to_stdout: true)

      # Path deps compile in place in modern Mix (no deps/flowex symlink) —
      # compiling proves the extracted artifact IS the dependency.
      {compile_out, 0} =
        System.cmd("mix", ["deps.compile"], cd: downstream, stderr_to_stdout: true)

      assert compile_out =~ "Generated flowex app", compile_out

      {fmt_out, 0} =
        System.cmd("mix", ["format", "--check-formatted"], cd: downstream, stderr_to_stdout: true)

      assert fmt_out == "", "downstream formatter rewrote the DSL form:\n#{fmt_out}"

      {run_out, 0} =
        System.cmd(
          "mix",
          [
            "run",
            "-e",
            ~s[pipeline = ConsumerApp.Pipeline.start()] <>
              ~s[\nresult = ConsumerApp.Pipeline.call(pipeline, %ConsumerApp.Pipeline{number: 21})] <>
              ~s[\nIO.puts("DOWNSTREAM_RESULT=" <> Integer.to_string(result.number))] <>
              ~s[\nrescued = ConsumerApp.Pipeline.call(pipeline, %ConsumerApp.Pipeline{number: :boom})] <>
              ~s[\nIO.puts("DOWNSTREAM_ERROR_ROUTE=" <> inspect(rescued.number))] <>
              ~s[\nslow = ConsumerApp.SlowPipeline.start()] <>
              ~s[\ntry do] <>
              ~s[\n  ConsumerApp.SlowPipeline.call(slow, %ConsumerApp.SlowPipeline{number: 1}, 10)] <>
              ~s[\n  IO.puts("DOWNSTREAM_TIMEOUT=did-not-raise")] <>
              ~s[\nrescue] <>
              ~s[\n  error in Flowex.PipelineError ->] <>
              ~s[\n    IO.puts("DOWNSTREAM_TIMEOUT=" <> inspect(error.reason))] <>
              ~s[\nend]
          ],
          cd: downstream,
          stderr_to_stdout: true
        )

      assert run_out =~ "DOWNSTREAM_RESULT=42", run_out
      assert run_out =~ "DOWNSTREAM_ERROR_ROUTE=:boom", run_out
      assert run_out =~ "DOWNSTREAM_TIMEOUT=:timeout", run_out
    after
      File.rm_rf!(tmp)
    end
  end
end
