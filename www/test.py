import orm
import asyncio
from models import User, Blog, Comment


async def test(loop):
    await orm.create_pool(loop=loop, user='root', password='mysql', db='blogs')
    tester = User(name='Test', email='test@qq.com', password='123456789', image='about:blank')