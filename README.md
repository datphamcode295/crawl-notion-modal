# Notion modal

```

You will then be able to use the `modal` cli on your machine. The Modal workspace is tied to this GitHub repository, get the API token and apply it to your Modal cli:

```sh
modal token set --token-id ak-u7kCLe6TlAgV0DhJgzg3xT --token-secret as-IdHL0u0TQ6j5yt4jgwGZQi  --profile=notion_modal
modal profile activate notion_modal
```

To deploy or test the application, simply run:
```sh
# Scrape the data
modal run scrape.py

# Deploy the FastAPI and Cron Jobs
modal deploy scrape.py

# Serve the FastAPI with hot reload
modal serve scrape.py
```