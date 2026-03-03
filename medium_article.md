# From Field Ecologist to Cloud Data Engineer in two weeks 
**Building a Fully Automated ETL Pipeline Using Google Cloud Platform**


I’ve spent years working with complex data - analysing patterns, building models, running simulations, visualising trends, and uncovering insights. But I’ve always collected data “in the field” (in my case, as an ecologist, quite literally).

Modern field ecology is already highly digitised. We use structured data-collection software on rugged devices, validate inputs in real time, and upload observations at the end of the day to centralised databases. The workflows are efficient and standardised.

But it still depends on us being there. which can be nice… if you don’t mind a bit of gruntwork

![Collecting data in the field, with cameras, notebooks, and monkeys](../images/fieldwork.JPG)
*Data collection on the ground*

Data are collected because someone walks into a landscape, observes something, records it, and synchronises it later. It is intentional, physical, and human-triggered.

At some point, I started wondering: what would it look like to design a system that collected data without me?

Over the past two weeks, I built exactly that - a fully automated ETL pipeline in the cloud. It scrapes websites, retrieves structured data from APIs, stores everything in a managed database, and updates itself on a schedule.

It wasn’t dramatic. It wasn’t overwhelmingly complex.  
And it turns out, building a pipeline can be surprisingly satisfying.

### Data Before Models

I worked on a toy project centred on Gans, a fictional e-scooter startup competing with companies like TIER and Bird. For Gans, operational success depends on something deceptively simple: scooters need to be available where users expect them.

Urban movement, however, is asymmetric. Commuters flow toward city centres in the morning. Tourists cluster near airports and landmarks. Rain suppresses demand almost instantly. In hilly cities, riders happily travel uphill and rarely return downhill.

Predictive modelling could help optimise redistribution. But modelling sits downstream of something more fundamental.

Reliable, continuously updated data.

Before prediction comes infrastructure.

### Designing a Simple System Properly

The final architecture of the pipeline integrated three external sources.

![Diagram of automated data pipeline connecting Wikipedia (for cities data), OpenWeather (for weather data), and AeroDataBox (for flights data) to a cloud database.](../images/datasources.png)
*All roads lead to the cloud: cities, weather, and flights funneling neatly into my MySQL database*

City-level data was scraped from Wikipedia locally, using BeautifulSoup and written into a cloud-hosted MySQL database.

Weather data came from the OpenWeather API. Flight arrival data was retrieved via the AeroDataBox API. Both were deployed as serverless functions in Google Cloud Platform, scheduled to run automatically and insert fresh records into the same database.

On paper, the architecture was straightforward:

Extract → Transform → Load → Automate.

The challenge wasn’t that it was technically overwhelming - it wasn’t. The challenge was discipline.

Writing code that works once is easy. Writing code that runs automatically, on schedule, without supervision, is different. It forces you to think about edge cases, structure, and reliability. It pushes you to treat your scripts not as one-off solutions, but as building blocks of a coherent, automated system.

That shift — from writing code to designing systems — was the real learning.

### Building Locally: Thinking in Relationships

I built the entire pipeline locally before touching the cloud. That decision shaped everything that followed.

Scraping required careful handling of HTML structure and response behaviour. APIs required authentication and careful navigation of nested JSON objects. Data cleaning meant standardising inconsistent strings and resolving missing values before insertion.

At the same time, I designed a relational schema in MySQL. This meant thinking explicitly about how entities relate: cities, weather observations, flight arrivals. It required defining primary keys, foreign keys, constraints, and data types that would preserve integrity over time.

![Relational SQL schema showing tables for cities, weather observations, and flight arrivals with primary and foreign key relationships](../images/schema.png)
*Relational schema showing how cities, weather, and flights tables are connected in MySQL*

In ecology, relationships matter - nothing exists in isolation. Ecosystems are collections of related, interconnected, interdependent organisms and environmental features. Designing database relationships felt unexpectedly familiar. The medium was different; the thinking was not.

By the time I migrated to the cloud, the logic was stable, and that made automation possible.

### When Code Becomes Infrastructure

Migrating to Google Cloud Platform added a new dimension.

Using Cloud SQL, I created a managed MySQL instance and configured secure connections between my local environment and the cloud database. Environment variables, network permissions, and credentials suddenly mattered as much as Python syntax.

Next, I refactored the API scripts into deployable units using Cloud Functions. Notebook-style experimentation had to become modular, self-contained functions with clearly defined entry points, while still drawing on credentials, configurations, and permissions defined elsewhere.

![HTTP 500 error cat — the face of every function failing to deploy on the first try.](../images/500.jpg)
*I’m in ur cloud, HTTP 500-ing ur functions.*

And then… the infamous HTTP 500 errors appeared. Every function I deployed returned a generic “Internal Server Error.” The scripts worked perfectly locally, but in the cloud, something was broken. I spent a couple of hours digging through logs, checking environment variables, and hunting down small differences that only mattered in the deployed environment.

It was frustrating in the moment, but also strangely fun — like a puzzle where all the pieces were familiar, but the shape had changed slightly. A few tweaks later, the functions ran flawlessly.

![GCP deployment success — green check marks all around. My functions are officially alive in the cloud!](../images/deployed.png)  
*Green check marks! Proof that even serverless code can behave nicely sometimes*

Finally, Cloud Scheduler automated execution. Weather and flight data began updating on schedule without intervention. In the morning, I woke up to hundreds of fresh rows in my database, my functions quietly doing their job while I slept.

The pipeline no longer required my presence.


### Automation as a Continuum

In hindsight, this wasn’t a departure from my background in ecology — it was an extension of it. In fact, my experience in field ecology became a strength for building data pipelines.

Ecology trains you to notice patterns in complex systems, think carefully about relationships, and validate observations rigorously. These skills translate directly into data engineering and data science: understanding how datasets interact, designing reliable workflows, and ensuring data integrity at scale.

Fieldwork already values structured data collection, validation, and centralised storage. What this project added was autonomy. Instead of uploading observations at the end of the day, the system updates itself. Instead of human-triggered synchronisation, functions execute on schedule.

The principles remain the same: observe carefully, structure deliberately, preserve integrity. The difference is scale, independence, and the new technical tools at your disposal.


### A Different Relationship with Data

Before this project, I believed the interesting work began once the dataset was ready.

Now I appreciate that building a pipeline that updates a database consistently, reliably, and automatically is not just a foundation for analysis — it’s interesting in its own right. Watching a pipeline run on schedule, delivering fresh, clean data without any manual intervention, is pretty satisfying!

Whether walking through a forest with a rugged tablet or deploying serverless functions in the cloud, the goal is the same: build trustworthy data foundations. The tools are different, the scale grows, but the principles remain.

Structured observation, careful validation, and preserving integrity — these are skills cultivated in the field that serve just as well in the cloud. Increasingly, the “field” extends beyond the physical landscape into systems we design to observe the world for us.

*If you're curious about the inner workings of the pipeline, the GitHub repo is available here: https://github.com/delmurphy/WBS-cloud-data-pipeline*