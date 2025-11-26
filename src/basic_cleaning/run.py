#!/usr/bin/env python
"""
Performs basic cleaning on the data and saves the results in my W&B project
"""
import argparse
import logging
import wandb
import pandas as pd


logging.basicConfig(level=logging.INFO, format="%(asctime)-15s %(message)s")
logger = logging.getLogger()


def go(args):

    run = wandb.init(job_type="basic_cleaning")
    run.config.update(args)

    # Download input artifact. This will also log that this script is using this
    # particular version of the artifact
    # artifact_local_path = run.use_artifact(args.input_artifact).file()

    logger.info("Downloading and reading artifact")
    artifact_local_path = run.use_artifact(args.input_artifact).file()
    df = pd.read_csv(artifact_local_path)

    logger.info("Dropping outliers")
    idx = df['price'].between(args.min_price, args.max_price)
    df = df[idx].copy()
    logger.info("Converting last_review to datetime")
    df['last_review'] = pd.to_datetime(df['last_review'])

    logger.info("Saving cleaned data to csv")
    df.to_csv("clean_sample.csv", index=False)

    logger.info("Creating and logging artifact")
    artifact = wandb.Artifact(
        args.output_artifact,
        type=args.output_type,
        description=args.output_description,
    )
    artifact.add_file("clean_sample.csv")
    run.log_artifact(artifact)

if __name__ == "__main__":

    parser = argparse.ArgumentParser(description="Preprocessing step for cleaning the data")


    parser.add_argument(
        "--input_artifact", 
        type=str,
        help="Fully qualified name for the artifact to be cleaned",
        required=True
    )

    parser.add_argument(
        "--output_artifact", 
        type=str,
        help="Name for the W&B artifact that will be created and store the cleaned data",
        required=True
    )

    parser.add_argument(
        "--output_type", 
        type=str,
        help='Type for the W&B artifact that will be created',
        required=True
    )

    parser.add_argument(
        "--output_description", 
        type=str,
        help="Description for the W&B artifact that will be created",
        required=True
    )

    parser.add_argument(
        "--min_price", 
        type=float,
        help="Minimum price for a rental to be included in the cleaned dataset",
        required=True
    )

    parser.add_argument(
        "--max_price", 
        type=float,
        help="Maximum price for a rental to be included in the cleaned dataset",
        required=True
    )


    args = parser.parse_args()

    go(args)
