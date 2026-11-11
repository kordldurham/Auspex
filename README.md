# Auspex external market scanner

This repository contains the browser-independent execution worker for the existing [Auspex miniature price monitor](https://auspex-mini-prices-k7m2.devkorddurham.chatgpt.site).

The application and authoritative catalog remain in their existing Sites deployment. This worker claims durable jobs, collects permitted merchant JSON, and acknowledges raw collection evidence before server-side matching. It does not host a replacement app or send email.

See [runner/README.md](runner/README.md) for installation, short-lived GitHub OIDC authentication, safe polling, interruption/resume, acceptance runs and portable Docker/systemd templates.

The workflow is published separately after backend repository trust is configured. Source code alone does not establish that production scheduling is running.
