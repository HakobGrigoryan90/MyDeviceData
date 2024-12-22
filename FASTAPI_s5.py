from fastapi import FastAPI, HTTPException, Query
import pandas as pd
from datetime import datetime

# Load the CSV file
try:
    df = pd.read_csv('device_data.csv', parse_dates=['Date'])
    df.set_index('Date', inplace=True)
    
    # Get the actual data range
    data_start = df.index.min().strftime('%Y-%m-%d')
    data_end = df.index.max().strftime('%Y-%m-%d')
except FileNotFoundError:
    raise RuntimeError("The file 'device_data.csv' was not found. Please ensure the file is in the correct location.")

# Create FastAPI app instance
app = FastAPI(title="Device Data API",
              description=f"This API provides access to device data from {data_start} to {data_end}.")

@app.get("/api/data_info")
async def get_data_info():
    return {
        "data_range": {
            "start": data_start,
            "end": data_end
        },
        "total_records": len(df)
    }

@app.get("/api/get_data_by_date")
async def get_data_by_date(
    date: str = Query(..., description="Date in format 'YYYY-MM-DD'")
):
    try:
        # Parse the input date
        query_date = datetime.strptime(date, '%Y-%m-%d').date()
        
        # Get the data for the specified date
        data = df.loc[query_date]
        
        # Prepare the response
        if isinstance(data, pd.Series):
            data = data.to_dict()
        else:
            data = data.to_dict('records')
        
        response = {
            "date": date,
            "data": data
        }
        
        return response
    
    except KeyError:
        raise HTTPException(status_code=404, detail="Data not found for the specified date")
    except ValueError:
        raise HTTPException(status_code=400, detail="Invalid date format. Please use 'YYYY-MM-DD'")
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"An error occurred: {str(e)}")

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8004)