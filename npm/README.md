# ThinkingLanguage

A purpose-built language for Data Engineering & AI.

This npm package installs the `tl` CLI binary by downloading the appropriate prebuilt release for your platform from [GitHub Releases](https://github.com/mplusm/thinkinglanguage/releases).

## Install

```bash
npx thinkinglanguage --help
```

Or install globally:

```bash
npm install -g thinkinglanguage
tl --help
```

## Supported platforms

- Linux x86_64
- macOS arm64 (Apple Silicon)
- Windows x86_64

## Quick example

```tl
let users = read_csv("users.csv")

users
    |> filter(age > 30)
    |> aggregate(by: department, count: count(), avg_age: avg(age))
    |> sort("count", "desc")
    |> show()
```

## Links

- [Repository](https://github.com/mplusm/thinkinglanguage)
- [Documentation](https://github.com/mplusm/thinkinglanguage/tree/main/docs)
- [Website](https://thinkingdbx.com)

## License

Apache-2.0
