#!/usr/bin/env python3
"""
Breakthrough Cannabis Scraper - Main Runner
A comprehensive web scraping tool for cannabis data collection from Weedmaps and other sources.

Author: Breakthrough Cannabis Analytics Team
Version: 1.0.0
"""

import sys
import os
import json
import argparse
import logging
from datetime import datetime
from pathlib import Path

# Add the current directory to Python path
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

try:
      from weedmaps_scraper import WeedmapsScraper
      from data_processor import DataProcessor
except ImportError as e:
      print(f"Error importing modules: {e}")
      print("Please ensure all required files are in the same directory")
      sys.exit(1)

def setup_logging():
      """Set up logging configuration."""
      log_dir = Path("logs")
      log_dir.mkdir(exist_ok=True)

    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    log_file = log_dir / f"breakthrough_scraper_{timestamp}.log"

    logging.basicConfig(
              level=logging.INFO,
              format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
              handlers=[
                            logging.FileHandler(log_file),
                            logging.StreamHandler(sys.stdout)
              ]
    )
    return logging.getLogger(__name__)

def load_config(config_path="config/scraper_config.json"):
      """Load scraper configuration."""
      try:
                with open(config_path, 'r') as f:
                              return json.load(f)
      except FileNotFoundError:
                logger.warning(f"Config file not found: {config_path}. Using default settings.")
                return {
                    "weedmaps": {
                        "base_url": "https://weedmaps.com",
                        "delay_range": [1, 3],
                        "max_retries": 3,
                        "timeout": 30
                    },
                    "output": {
                        "format": "json",
                        "directory": "data",
                        "filename_template": "breakthrough_data_{timestamp}.json"
                    }
                }

  def main():
        """Main execution function."""
        parser = argparse.ArgumentParser(description="Breakthrough Cannabis Scraper")
        parser.add_argument("--location", default="california", help="Location to scrape (default: california)")
        parser.add_argument("--product-type", default="flower", help="Product type to scrape (default: flower)")
        parser.add_argument("--max-pages", type=int, default=5, help="Maximum pages to scrape (default: 5)")
        parser.add_argument("--output-dir", default="data", help="Output directory (default: data)")
        parser.add_argument("--config", default="config/scraper_config.json", help="Config file path")
        parser.add_argument("--validate", action="store_true", help="Run validation after scraping")
        parser.add_argument("--verbose", "-v", action="store_true", help="Enable verbose logging")

    args = parser.parse_args()

    # Setup logging
    global logger
    logger = setup_logging()

    if args.verbose:
              logging.getLogger().setLevel(logging.DEBUG)

    logger.info("Starting Breakthrough Cannabis Scraper")
    logger.info(f"Parameters: Location={args.location}, Product Type={args.product_type}, Max Pages={args.max_pages}")

    try:
              # Load configuration
              config = load_config(args.config)
              logger.info("Configuration loaded successfully")

        # Create output directory
              output_dir = Path(args.output_dir)
              output_dir.mkdir(exist_ok=True)

        # Initialize scraper
              scraper = WeedmapsScraper(config=config.get("weedmaps", {}))
              logger.info("Weedmaps scraper initialized")

        # Perform scraping
              logger.info("Starting data collection...")
              scraped_data = scraper.scrape_products(
                  location=args.location,
                  product_type=args.product_type,
                  max_pages=args.max_pages
              )

        logger.info(f"Successfully scraped {len(scraped_data)} products")

        # Process data
        processor = DataProcessor(config=config.get("output", {}))
        processed_data = processor.process_and_validate(scraped_data)

        # Save results
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        output_file = output_dir / f"breakthrough_data_{timestamp}.json"

        with open(output_file, 'w', encoding='utf-8') as f:
                      json.dump(processed_data, f, indent=2, ensure_ascii=False)

        logger.info(f"Data saved to: {output_file}")

        # Run validation if requested
        if args.validate:
                      logger.info("Running data validation...")
                      from validate_package import run_validation
                      validation_results = run_validation(str(output_file))
                      logger.info(f"Validation completed: {validation_results}")

        logger.info("Breakthrough scraper completed successfully!")

        # Print summary
        print("\n" + "="*50)
        print("BREAKTHROUGH SCRAPER SUMMARY")
        print("="*50)
        print(f"Products scraped: {len(scraped_data)}")
        print(f"Products processed: {len(processed_data)}")
        print(f"Output file: {output_file}")
        print(f"Execution time: {datetime.now()}")
        print("="*50)

except KeyboardInterrupt:
        logger.info("Scraper interrupted by user")
        sys.exit(0)
except Exception as e:
        logger.error(f"Error during scraping: {str(e)}", exc_info=True)
        sys.exit(1)

if __name__ == "__main__":
      main()
