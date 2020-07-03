aggregator.py is used to make an mbox file of the emails which have failed to migrate from one email service to another.

Steps involved - 
 * Download the mbox files from both original email and migrated email servers
 * Run aggregator.py with following command - `python3 aggregator.py /path/to/original/mbox /path/to/migrated/mbox /path/to/unmigrated/mbox/emails/to/be/created  file_flag_for_original_mbox(rb by default)` 
    