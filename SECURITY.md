# Security

I believe security issues are welcome and should be reported responsibly.

## Reporting a Vulnerability

For sensitive vulnerabilities that could expose credentials, user data allow unauthorized access or enable code execution please **do not open a public GitHub issue**.

Instead I recommend you report them privately to the project maintainer.

Please include:

* version

* Short description

* Reproduction steps

* Potential impact

## Public Security Issues

For non-sensitive security bugs hardening improvements or issues that do not expose exploitable details you may open a **GitHub issue**.

When reporting publicly avoid including credentials, private data exploit details or other sensitive information.

## Security Considerations

* Keep database and Redis credentials outside source code.

* Avoid storing secrets in job payloads.

* Workers execute user-defined JavaScript. Do not sandbox untrusted code.

* Use connections and authentication, for production storage backends.