#!/bin/bash

# parse yaml file and set crontab
smhi_command="./invoke_ingestor.sh"
# save the result to a variable
scheduler=$(cat scheduler.yaml | shyaml keys living_lab.scheduler)
echo $scheduler
echo  "-----------"
# loop through the scheduler keys
for key in $scheduler
    do
        echo $key
        # extract the scheduler values
        echo "Extracting values"
        # parse frequency in crontab format
        freq=$(cat scheduler.yaml | shyaml get-value living_lab.scheduler.$key.frequency)
        echo "FREQ: $freq"
        echo "-----------"
        
        # Write the current crontab to a temporary file
        crontab -l > mycron

        # Add new cron jobs to the temporary file
        echo "Creating new cron job $freq $smhi_command"
        echo "$freq $smhi_command" >> mycron
        
        # Install the new cron file
        crontab mycron

        # Remove the temporary file
        rm mycron

        # Output the parsed data and crontab set messages
        echo "Frequency: $freq - Crontab set"
        
    done


