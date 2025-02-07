# SommScore

SommScore is a web application for tracking and analyzing sales associate performance metrics in a wine retail environment. It calculates daily performance scores based on sales data, club signups, and other metrics to provide insights into individual and team performance.

## Features

- Daily performance scoring system
- Team and individual performance tracking
- Interactive trend visualization
- Fiscal or calendar year tracking
- Club signup and revenue analysis
- Dark mode support
- Responsive design

## Installation

1. Clone the repository:
```bash
git clone <repository-url>
cd SommScore
```

2. Create and activate a virtual environment:
```bash
python -m venv venv
source venv/bin/activate  # On Windows: venv\Scripts\activate
```

3. Install dependencies:
```bash
pip install -r requirements.txt
```

4. Set up environment variables:
Create a `.env` file in the root directory with the following variables:
```
DB_PATH=data/commerce7.db
```

5. Initialize the database:
```bash
python init_db.py
```

## Running the Application

1. Start the Flask development server:
```bash
python app.py
```

2. Open a web browser and navigate to `http://localhost:5000`

## Docker Support

The application can also be run using Docker:

```bash
docker-compose up --build
```

## Configuration

- Year Type: Choose between fiscal or calendar year for calculations
- Active Associates: Select which associates to display on the dashboard
- Display Options: Toggle dark mode and other display preferences

## License

[Your chosen license] 