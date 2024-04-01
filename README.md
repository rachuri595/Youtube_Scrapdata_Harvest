# Youtube_Scrapdata_Harvest
**Project Overview: YouTube Data Harvesting and Warehousing**

**Project Summary:**
This project focuses on extracting data from YouTube channels using Python, specifically utilizing the Google API for YouTube. The harvested data is then transformed and stored in a MongoDB database, followed by conversion and warehousing in a MySQL server. Additionally, an interactive web application is created using Streamlit to visualize insights derived from the harvested data.

**Project Objectives:**
- Extract data from multiple YouTube channels, including channel details, video details, and comments.
- Store the harvested data in a MongoDB database for initial storage and processing.
- Transform and warehouse the data into a MySQL server for long-term storage and querying.
- Develop an interactive web application using Streamlit to query and visualize insights from the harvested YouTube data.

**Business Goals:**
- Ensure proper authentication and quota management for accessing the YouTube API.
- Optimize data retrieval methods to minimize API quota consumption and enhance efficiency.
- Implement robust error handling and logging mechanisms to manage API request failures and data processing errors effectively.
- Regularly monitor and maintain the MongoDB and MySQL databases to ensure data integrity and performance.
- Enhance the Streamlit web application with features like user authentication, caching, and data export capabilities.

**Project Implementation Steps:**
1. **Set Up API Keys:** Obtain API keys for the YouTube Data API from the Google Developer Console.
2. **Data Retrieval:** Harvest YouTube channel details, video details, and comments using the YouTube API.
3. **Data Storage (MongoDB):** Store the harvested data in a MongoDB database for temporary storage.
4. **Data Transformation:** Transform the raw data into a suitable format for warehousing in a relational database.
5. **Data Warehousing (MySQL):** Create tables in a MySQL server and upload the transformed data for long-term storage.
6. **Streamlit Development:** Develop an interactive web application using Streamlit for querying and visualizing insights from the warehoused data.
7. **Testing and Deployment:** Thoroughly test the application and deploy it to a suitable hosting platform.

**Getting Started:**
To run the project locally, follow these steps:

1. Clone this repository to your local machine.
2. Install the required Python dependencies listed in word document.
3. Obtain API keys for the YouTube Data API and MongoDB Atlas (optional).
4. Configure the API keys and database connection settings in the project files.
5. Run the Streamlit web application using the command `streamlit run app.py`.
6. Access the application through the provided local URL in your web browser.

**Conclusion:**
This project aims to provide a comprehensive solution for harvesting, storing, and visualizing YouTube data, offering valuable insights for various analytical purposes. With proper implementation and maintenance, it can serve as a powerful tool for data-driven decision-making and content analysis in the digital media domain.
