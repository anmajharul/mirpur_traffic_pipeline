# Koyeb Cron Job Setup Guide (5-Minute Data Collection)

We have removed the old Google Cloud Run deployment pipeline from GitHub Actions. Your machine learning models will continue to train automatically on GitHub Actions as they did before.

To set up the **5-minute interval data collection** using Koyeb, follow these simple steps using your existing code. Koyeb's "Cron Jobs" feature natively runs Dockerfiles on a schedule, which is perfectly suited for your setup.

## Prerequisites
1. Ensure your latest code is pushed to your GitHub `main` branch.
2. Sign up or log into [Koyeb](https://app.koyeb.com/).

## Step-by-Step Instructions

1. **Create a New Service in Koyeb:**
   - In the Koyeb control panel, click **Create Service**.
   - Select **GitHub** as the deployment method.
   - Authorize Koyeb to access your GitHub repositories and select your `mirpur-10-research` (or equivalent) repository.

2. **Configure the Service Type (Crucial Step):**
   - Scroll down to the **Service type** section.
   - **DO NOT** select "Web Service" or "Worker".
   - Select **Cron Job**.

3. **Set the Cron Schedule:**
   - In the **Cron schedule** box, type exactly the following to run every 5 minutes:
     ```
     */5 * * * *
     ```

4. **Configure the Build Details:**
   - Under **Builder**, select **Dockerfile**.
   - In the **Dockerfile location** field, override the default and specify:
     ```
     Dockerfile.collector
     ```
   - (Leave the run command empty. Koyeb will automatically use the `ENTRYPOINT ["python", "run_collection.py"]` defined inside the Dockerfile).

5. **Set Environment Variables:**
   - Expand the **Environment variables** section and add the required secrets (same as the ones in your `.env` or GitHub Secrets):
     - `SUPABASE_URL`
     - `SUPABASE_KEY`
     - `MAPBOX_TOKEN`
     - `WEATHER_API_KEY`

6. **Choose Instance Size and Deploy:**
   - Under **Instance**, select `Eco` (Free tier is perfectly fine for this small script).
   - Give your service a name like `mirpur-collector-cron`.
   - Click **Deploy**.

## What Happens Next?
- Koyeb will build the `Dockerfile.collector` image automatically directly from your GitHub repo.
- Once built, Koyeb will launch the container **exactly once every 5 minutes**.
- The script `run_collection.py` will run, hit the APIs, insert data into Supabase, and immediately exit gracefully.
- You can monitor the live execution logs directly from the Koyeb dashboard.

You're done! Your high-frequency data collection is now fully automated on Koyeb, while your machine learning models continue to train on GitHub Actions.
