import hashlib


def generate_cb_uuid(tb_name, unique_id):
    instance_str = bytes("initial-{}-{}".format(tb_name, unique_id), 'utf-8')
    hash_str = hashlib.md5(instance_str).hexdigest()
    return "{}-{}-{}-{}-{}".format(hash_str[:8], hash_str[8:12], hash_str[12:16], hash_str[16:20], hash_str[20:])
