import datetime
from multiprocessing import Process, Queue
import sys


def aggregator(queue, filename, file_flag):
    def get_email_date(line, date_start_ix):
        email_date = line[date_start_ix:]
        return datetime.datetime.strptime((email_date.strip()), '%a %b %d %H:%M:%S %z %Y')

    aggregate = {}
    message_id_ix = 11
    message_id = 'Message-ID: '
    date_start_ix = 29
    with open(filename, file_flag) as f:
        key = None
        email = []
        email_date = None
        for line in f.readlines():
            try:
                line_ascii = line.decode('ascii')
            except UnicodeDecodeError:
                email.append(line)
            else:
                if line_ascii[:5] == 'From ':
                    if len(email) == 0:
                        email_date = get_email_date(line_ascii, date_start_ix)
                    else:
                        aggregate[key] = {
                            'email': email[:],
                            'email_date': email_date
                        }
                        key = None
                        email_date = get_email_date(line_ascii, date_start_ix)
                        email = []
                elif line_ascii.startswith(message_id):
                    key = line_ascii[message_id_ix:].strip()
                    aggregate[key] = {}
                email.append(line)

    print(filename, ' aggregated')
    queue.put(aggregate)
    return

def migrate_missing(original_email_mbox, migrated_email_mbox, out_mbox_name, file_flag='rb'):
    original_email_aggr_queue = Queue()
    migrated_email_aggr_queue = Queue()
    original_email_aggr_process = Process(target=aggregator, args=(original_email_aggr_queue,
                                                               original_email_mbox,
                                                               file_flag)
                                          )
    migrated_email_aggr_process = Process(target=aggregator, args=(migrated_email_aggr_queue,
                                                               migrated_email_mbox,
                                                               file_flag)
                                          )
    original_email_aggr_process.start()
    migrated_email_aggr_process.start()
    original_email_aggr = original_email_aggr_queue.get()
    migrated_email_aggr = migrated_email_aggr_queue.get()

    message_ids_not_migrated = set(original_email_aggr.keys()).difference(set(migrated_email_aggr.keys()))
    emails_to_migrate = [original_email_aggr[msg_id] for msg_id in message_ids_not_migrated]
    emails_to_migrate = list(filter(lambda obj: len(list(obj.keys())) != 0, emails_to_migrate))
    print(len(emails_to_migrate), ' emails were missed out during migration')
    print('sorting emails according to date')
    for email in emails_to_migrate:
        print(email['email_date'])
        break
    sorted_emails_to_migrate = sorted(emails_to_migrate, key=lambda i: i['email_date'])
    if len(sorted_emails_to_migrate):
        with open(out_mbox_name, 'wb') as f:
            for obj in sorted_emails_to_migrate:
                if len(list(obj.keys())) == 0:
                    print(obj)

                f.writelines(obj['email'])

        print('process complete.', len(sorted_emails_to_migrate), ' migrated. Kindly check ', out_mbox_name)
    else:
        print('No emails required to migrate')


if __name__ == '__main__':
    original_mbox = sys.argv[1]
    migrated_mbox = sys.argv[2]
    out_mbox = sys.argv[3]
    try:
        file_flag = sys.argv[4]
    except IndexError:
        file_flag = None
    migrate_missing(original_mbox, migrated_mbox, out_mbox, file_flag)
