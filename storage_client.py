from scm_api import ScmSession

if __name__ == '__main__':
    session = ScmSession('Username', 'Password', '127.0.0.1', verify_cert=False)
    session.login()
