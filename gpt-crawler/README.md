# Crawler Module

This module crawls documentation or product websites and produces structured text output that can be used as contextual knowledge for AI call agents.

## Purpose in This Repository

The crawler is used during agent setup flows to extract public-facing website content and condense it into prompt-ready context.

## Runtime Modes

- CLI mode for one-off crawls
- API mode for programmatic crawl jobs
- Container mode for isolated execution

## Local Usage

1. Install dependencies:

```bash
npm install
```

2. Configure crawling parameters in `config.ts`:

- `url`: initial page to crawl
- `match`: allowed URL pattern
- `selector`: DOM selector for main content extraction
- `maxPagesToCrawl`: crawl cap
- `outputFileName`: output artifact path

3. Run crawler:

```bash
npm start
```

## API Usage

Start server:

```bash
npm run start:server
```

Then send a POST request to `/crawl` with crawl config payload.

## Container Usage

See `containerapp/README.md` for containerized execution.

## Operational Notes

- Keep crawl scope narrow to reduce noisy content.
- Exclude static assets and binary extensions in config.
- Do not commit generated crawl outputs.
