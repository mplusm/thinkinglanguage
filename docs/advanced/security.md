# Security and Access Control

TL includes built-in security features for handling sensitive data, restricting execution capabilities, and masking confidential information. These features are designed for production data engineering workflows where data privacy and access control are critical.

## Secret Vault

The secret vault provides secure storage for sensitive values such as API keys, database passwords, and tokens. Secret values are wrapped in a special `Secret` type that prevents accidental exposure.

### Storing Secrets

```
secret_set("api_key", "sk-abc123def456")
secret_set("db_password", "super_secret_password")
```

### Retrieving Secrets

```
let key = secret_get("api_key")
print(key)  // Outputs: ***
```

Secret values always display as `***` when printed, logged, or otherwise converted to string representation. The underlying value is accessible only to functions that explicitly handle Secret types (such as connectors and HTTP clients).

### Managing Secrets

```
// List all stored secret names
let names = secret_list()

// Delete a secret
secret_delete("api_key")
```

## Data Masking Functions

Built-in functions for masking personally identifiable information (PII) and other sensitive data.

### mask_email

Masks an email address, preserving the first character and domain structure.

```
mask_email("user@example.com")    // "u***@example.com"
```

### mask_phone

Masks a phone number, preserving only the last four digits.

```
mask_phone("555-123-4567")        // "***-***-4567"
```

### mask_cc

Masks a credit card number, preserving only the last four digits.

```
mask_cc("4111111111111111")       // "****-****-****-1111"
```

### redact

Custom redaction using a pattern.

```
redact("My SSN is 123-45-6789", "\\d{3}-\\d{2}-\\d{4}")
// "My SSN is [REDACTED]"
```

## Hashing

Compute cryptographic hashes of values.

```
let h = hash("sensitive data")            // SHA-256 hash (default)
let h_md5 = hash("sensitive data", "md5") // MD5 hash
```

## @sensitive Annotation

Mark struct fields as sensitive. Sensitive fields are automatically masked when the struct is displayed or logged.

```
struct User {
    name: string
    @sensitive
    ssn: string
    @sensitive
    credit_card: string
}

let user = User { name: "Alice", ssn: "123-45-6789", credit_card: "4111111111111111" }
print(user)
// User { name: "Alice", ssn: ***, credit_card: *** }
```

## Sandbox Mode

Sandbox mode restricts the capabilities of a TL script, preventing file writes and network access. This is useful for running untrusted code or enforcing least-privilege execution.

### Enabling Sandbox

```
tl run script.tl --sandbox
```

In sandbox mode:

- File write operations are blocked.
- Network access is blocked.
- Connectors (database, HTTP, etc.) are blocked by default.

### Allowing Specific Connectors

Use the `--allow-connector` flag to whitelist specific connectors in sandbox mode. This flag can be repeated to allow multiple connectors.

```
tl run script.tl --sandbox --allow-connector postgres
tl run script.tl --sandbox --allow-connector postgres --allow-connector redis
```

### Security Policy

The security policy is enforced at the VM level. When sandbox mode is active, the VM checks each operation against the policy before execution. Blocked operations raise a runtime error.

## Implementation Details

### VmValue::Secret and Value::Secret

Both the VM and the interpreter have dedicated Secret value types:

- `VmValue::Secret` -- used in the bytecode VM
- `Value::Secret` -- used in the tree-walking interpreter

These types wrap an inner value and override display formatting to always show `***`. The inner value is only unwrapped by authorized operations (such as connector authentication and HTTP headers).

### Hashing Implementation

- SHA-256 hashing uses the `sha2` crate.
- MD5 hashing uses the `md-5` crate.
