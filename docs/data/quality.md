# Data Quality

TL provides built-in functions for data cleaning, profiling, and validation. These integrate directly with pipe operations for composable data quality workflows.

## Data Cleaning

### fill_null

Replace null values in a column with a default:

```tl
data |> fill_null("region", "unknown")
```

### drop_null

Remove all rows containing null values:

```tl
data |> drop_null()
```

### dedup

Remove duplicate rows, optionally by a specific column:

```tl
data |> dedup()
data |> dedup(by: email)
```

### clamp

Restrict column values to a range:

```tl
data |> clamp("temperature", 0.0, 100.0)
```

## Data Profiling

### data_profile

Generate comprehensive statistics for a table:

```tl
let stats = data_profile(table)
```

### row_count

Get the number of rows:

```tl
let n = row_count(table)
```

### null_rate

Get the percentage of null values in a column:

```tl
let rate = null_rate(table, "email")
```

### is_unique

Check if all values in a column are unique:

```tl
let unique = is_unique(table, "id")
```

## Validation Functions

### is_email

Validate email format:

```tl
data |> filter(is_email(email))
```

### is_url

Validate URL format:

```tl
data |> filter(is_url(website))
```

### is_phone

Validate phone number format:

```tl
data |> filter(is_phone(phone))
```

### is_between

Range check for a value:

```tl
data |> filter(is_between(age, 18, 65))
```

## String Similarity

### levenshtein

Compute the edit distance between two strings:

```tl
let dist = levenshtein("hello", "hallo")
```

### soundex

Generate a phonetic encoding of a string:

```tl
let code = soundex("Robert")
```

## Pipeline Integration

Chain quality checks in pipe operations for end-to-end data cleaning:

```tl
data
    |> drop_null()
    |> dedup(by: email)
    |> fill_null("region", "unknown")
    |> filter(is_email(email))
    |> show()
```

This pipeline removes rows with nulls, deduplicates by email, fills remaining null regions with a default, validates email format, and displays the result.
