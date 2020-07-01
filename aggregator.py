import datetime
from multiprocessing import Process, Queue
import sys
import pickle


def get_email_date(_line, _date_start_ix):
    _email_date = _line[_date_start_ix:]
    return datetime.datetime.strptime((_email_date.strip()), '%a %b %d %H:%M:%S %z %Y')


def get_message_ids_time_set(queue, filename, _file_flag):
    message_ids_time_set = set()
    message_id_ix = 11
    message_id_label = 'Message-ID'
    date_start_ix = 29
    email_begin_label = 'From '
    email_begin_label_last_ix = len(email_begin_label)
    with open(filename, _file_flag) as f:
        email_date = None
        for line in f:
            try:
                line_ascii = line.decode('ascii')
            except UnicodeDecodeError:
                continue
            else:
                if line_ascii.startswith(message_id_label):
                    msg_id = line_ascii[message_id_ix:].strip()
                    message_ids_time_set.add(msg_id)
    print('got message ids and corresponding dates for ', filename)
    queue.put(message_ids_time_set)


def collect_missing_emails(orig_mbox, pickle_file, out_mbox_name):
    message_id_ix = 11
    message_id_label = 'Message-ID'
    email_begin_label = 'From '
    email_begin_label_last_ix = len(email_begin_label)
    with open(pickle_file, 'rb') as f:
        missing_msg_ids_set = pickle.load(f)

    email_body = []
    with open(orig_mbox, 'rb') as f:
        with open(out_mbox_name, 'wb') as om:
            msg_id = None
            for line in f:
                try:
                    line_ascii = line.decode('ascii')
                except UnicodeDecodeError:
                    email_body.append(line)
                else:
                    if line_ascii.startswith(message_id_label):
                        msg_id = line_ascii[message_id_ix:].strip()
                        email_body.append(line)
                    elif line_ascii[:email_begin_label_last_ix] == email_begin_label and len(email_body) == 0:
                        email_body.append(line)
                    elif line_ascii[:email_begin_label_last_ix] == email_begin_label and len(email_body) and msg_id is\
                            not None:
                        if msg_id in missing_msg_ids_set:
                            om.writelines(email_body)
                            print('wrote email for Message-ID ', msg_id)
                            email_body = []
                            msg_id = None
                        else:
                            msg_id = None
                            email_body = []
                    else:
                        email_body.append(line)


def migrate_missing(original_email_mbox, migrated_email_mbox, out_mbox_name, pickled_filename,
                    _file_flag='rb', pickle_file=None):
    if pickle_file is not None:
        return collect_missing_emails(original_email_mbox, pickle_file, out_mbox_name)

    original_email_process_queue = Queue()
    migrated_email_process_queue = Queue()

    orig_email_process = Process(target=get_message_ids_time_set, args=(original_email_process_queue,
                                                                        original_email_mbox,
                                                                        _file_flag))
    migrated_email_process = Process(target=get_message_ids_time_set, args=(migrated_email_process_queue,
                                                                        migrated_email_mbox,
                                                                        _file_flag))
    orig_email_process.start()
    migrated_email_process.start()

    orig_msg_ids_time_set = original_email_process_queue.get()
    mig_msg_ids_time_set = migrated_email_process_queue.get()

    missed_emails = orig_msg_ids_time_set.difference(mig_msg_ids_time_set)
    print(len(missed_emails), ' failed to migrate')
    with open(pickled_filename, 'wb') as f:
        pickle.dump(missed_emails, f)
    collect_missing_emails(original_email_mbox, pickled_filename, out_mbox_name)


if __name__ == '__main__':
    original_mbox = sys.argv[1]
    migrated_mbox = sys.argv[2]
    out_mbox = sys.argv[3]
    pickled_filename = sys.argv[4]
    try:
        file_flag = sys.argv[5]
    except IndexError:
        file_flag = 'rb'
    try:
        pickle_file = sys.argv[6]
    except IndexError:
        pickle_file = None
    migrate_missing(original_mbox, migrated_mbox, out_mbox, pickled_filename, file_flag, pickle_file)
