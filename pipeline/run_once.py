"""Run the pipeline a single time, without the scheduler: python -m pipeline.run_once"""

from pipeline.flows import weather_etl

if __name__ == "__main__":
    print(weather_etl())
