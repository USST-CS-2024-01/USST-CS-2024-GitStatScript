import paramiko


def generate_ssh_key(key_filename):
    # 创建一个新的密钥对
    key = paramiko.RSAKey.generate(2048)
    # 将私钥写入文件
    key.write_private_key_file(key_filename)
    # 将公钥写入文件
    with open(f'{key_filename}.pub', 'w') as public_key_file:
        public_key_file.write(f'{key.get_name()} {key.get_base64()}')
    return key


if __name__ == '__main__':
    generate_ssh_key('test_ssh_key')
