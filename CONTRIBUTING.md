# Contributing to RisingLight

- [Contributing to RisingLight](#contributing-to-risinglight)
  - [Architecture Design](#architecture-design)
  - [Build and Run RisingLight](#build-and-run-risinglight)
  - [Create Tracking Issue if Necessary](#create-tracking-issue-if-necessary)
  - [Write Documentation](#write-documentation)
  - [Write Tests](#write-tests)
  - [Running Test and Checks](#running-test-and-checks)
  - [Sign DCO (Developer Certificate of Origin)](#sign-dco-developer-certificate-of-origin)
  - [Send Pull Requests](#send-pull-requests)

Thanks for your contribution! The RisingLight project welcomes contribution of various types -- new features, bug fixes
and reports, typo fixes, etc. If you want to contribute to the RisingLight project, you will need to pass necessary
checks and sign DCO. If you have any question, feel free to ping community members on GitHub and in Slack channels.

Besides sending Pull Requests, you may also take part in our community scrum meeting (see [README](README.md) for more information), chat in Slack channels, and become a RisingLight member (see [GOVERNANCE](GOVERNANCE.md) for more information).

## Architecture Design

You may take a look at the [architecture overview](./docs/03-architecture-overview.md) to have a better idea of RisingLight's design.

## Build and Run RisingLight

Please refer to [Install, Run, and Develop RisingLight](./docs/00-develop.md) for more information.

## Create Tracking Issue if Necessary

If you are working on a large feature (>= 300 LoCs), it is recommended to create a tracking issue first, so that
contributors and maintainers can understand the issue better and discuss how to proceed and implement the features.

## Write Documentation

Developers are recommended to document their code using Rust docstring. You can use `///` to document functions,
fields, and structs.

```test
/// This is a struct for test uses.
pub struct TestStruct;
```

To see if your docs render correctly, run:

```shell
make docs
```

In the poped-up web page, you may see how your docs render in HTML.

## Write Tests

Developers are recommended to add unit tests for the project. Use `#[test]` or `#[tokio::test]` to create test cases.

At the same time, developers may also add end-to-end tests with sqllogictest. You may follow the examples in
`tests/sql` and write sqllogictest to run SQLs in RisingLight and to verify implementation correctness.
All the files suffix with `.slt` but not prefix with `_` in `tests/sql` will be automatically included in the end-to-end tests.

See [SQLLogicTest and SQLPlannerTest](docs/05-e2e-tests.md) for more information.

You'll need `cargo install cargo-nextest` to run tests.

## Running Test and Checks

We provide a simple make command to run all the checks:

```shell
make check
```

The `make check` command contains the following parts:

* `cargo fmt --all -- --check` ensures your code is well-formatted.
  * If this check fails, simply run `cargo fmt` and all code will be formatted.
* `cargo clippy --workspace --all-features --all-targets` ensures some best practices in Rust.
  * If this check fails, follow the reported warnings and fix your code.
  * You can also use `cargo clippy --workspace --all-features --all-targets --fix` to automatically fix clippy issues. Note that
    some warnings cannot be automatically fixed.
* `cargo build --all-features --all-targets` builds the project.
  * We assume all warnings as errors, so you will need to fix warnings for your changes.
  * At the same time, this build also checks for compile error for optional features (like SIMD) and benchmark code.
    If your changes break benchmark code, you will also need to fix it.
* `cargo test --workspace --all-features` runs all unit tests.
  * If any unit test fails, there might be some logical error in your changes.

After all the checks pass, your changes will likely be accepted.

## Sign DCO (Developer Certificate of Origin)

Contributors will need to sign DCO in their commits. From [GitHub App's DCO](https://github.com/apps/dco) page:

The Developer Certificate of Origin (DCO) is a lightweight way for contributors to certify that they wrote or otherwise
have the right to submit the code they are contributing to the project. Here is the full text of the DCO, reformatted
for readability:

> By making a contribution to this project, I certify that:
> 
> The contribution was created in whole or in part by me and I have the right to submit it under the open source license indicated in the file; or
> 
> The contribution is based upon previous work that, to the best of my knowledge, is covered under an appropriate open source license and I have the right under that license to submit that work with modifications, whether created in whole or in part by me, under the same open source license (unless I am permitted to submit under a different license), as indicated in the file; or
> 
> The contribution was provided directly to me by some other person who certified 1., 2. or 3. and I have not modified it.
> 
> I understand and agree that this project and the contribution are public and that a record of the contribution (including all personal information I submit with it, including my sign-off) is maintained indefinitely and may be redistributed consistent with this project or the open source license(s) involved.

Contributors will need to add a `Signed-off-by` line in all their commits:

```
Signed-off-by: Random J Developer <random@developer.example.org>
```

The `git` command provides `-s` parameter to attach DCO to the commits.

```
git commit -m "feat(scope): commit messages" -s
```

## Send Pull Requests

After all checks pass and DCO gets signed, developers may fork the repo and create a pull request. You may describe the
change in the PR body so that other developers can understand your changes better.

The PR title should follow [Semantic Commit Messages](https://gist.github.com/joshbuchea/6f47e86d2510bce28f8e7f42ae84c716):


> `<type>(<scope>): <subject>`
>
> ```
> feat(scope): add hat wobble
> ^--^ ^---^   ^------------^
> |    |       |
> |    |       +-> Summary in present tense.
> |    |
> |    +---> Scope: executor, storage, etc.
> |
> +-------> Type: chore, docs, feat, fix, refactor, style, or test.
> ```
