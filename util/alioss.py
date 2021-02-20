# encoding: utf-8
import oss2


class AliOSS:
    def __init__(self):
        self.bucket = None

    def init(self, local_debug=False):
        if local_debug:
            return
        # 阿里云主账号AccessKey拥有所有API的访问权限，风险很高。强烈建议您创建并使用RAM账号进行API访问或日常运维，请登录 https://ram.console.aliyun.com 创建RAM账号。
        auth = oss2.Auth('LTAIuRlBvUKMjDP6', '99xAGacf4pBVNRMPSfD5ukzx98qm4h')
        self.bucket = oss2.Bucket(auth, 'oss-cn-hongkong-internal.aliyuncs.com', 'eaas')

    def sign_url(self, filename):
        link = self.bucket.sign_url('GET', filename, 100 * 365 * 24 * 60 * 60)  # 100年有效期
        return link.replace('oss-cn-hongkong-internal.aliyuncs.com', 'oss.amberainsider.com')

    def delete_if_exist(self, filename):
        if self.bucket.object_exists(filename):
            self.bucket.delete_object(filename)

    def update_pos_if_exist(self, filename):
        if self.bucket.object_exists(filename):
            meta = self.bucket.get_object_meta(filename)
            return int(meta.headers['Content-Length'])
        return 0

    def file_append(self, filename, pos, content):
        if self.bucket:
            result = self.bucket.append_object(filename, pos, content)
            return result.next_position
        return 0
    
    def file_download(self, url_link, filename):
        if self.bucket:
            ff = self.bucket.get_object_with_url_to_file(url_link, filename)
            return ff
        return 0


alioss = AliOSS()
