# 数据库orm定义

import asyncio, logging, aiomysql


# 创建异步数据连接池
async def create_pool(loop, **kw):
    # 直接日志记录
    logging.info("create database connecting pool ...")
    # 全局变量__pool存储，自动提交事务
    global __pool
    __pool = await aiomysql.create_pool(
        host=kw.get('host', 'localhost'),
        port=kw.get('port', 3306),
        user=kw['user'],
        password=kw['password'],
        db=kw['db'],
        charset=kw.get('charset', 'utf8'),
        autocomit=kw.get('autocommit', True),
        maxsize=kw.get('maxsize', 10),
        minsize=kw.get('minsize', 1),
        loop=loop
    )


# 定义数据库 select 语句， 传入sql语句和sql参数
async def select(sql, args, size=None):
    # Todo: 编写此段异步程序的过程中，如何确认哪一步需要使用到异步调用？
    # 从配置文件中导入日志记录
    log(sql, args)
    global  __pool
    # 异步获取连接池数据
    with (await __pool) as conn:
        # 异步连接数据库游标
        cur = await conn.cursor(aiomysql.DictCursor)
        # 异步执行sql 语句， 使用带参数的SQL语句防止SQL注入
        # cursor.execute('insert into user (id, name) values (%s, %s)', ['1', 'Michael'])
        # 替换 SQL 的占位符‘？’，使用MySQL的‘%s', 传入参数或者默认为空
        await cur.execute(sql.replace('?', '%s'), args or ())
        # 异步指定查询数目的记录
        if size:
            rs = await cur.fetchmany(size)
        else:
            rs = await cur.fetchall()
        # 异步关闭游标
        await cur.close()
        # 记录查询数目
        logging.info('rows returned: %s' % len(rs))
        return rs


# 定义数据库 insert, update, delet 语句， 传入sql语句和参数
async def execute(sql, args):
    log(sql)
    with (await __pool) as conn:
        try:
            # Todo: 数据库中获取游标方式是什么？
            cur = await conn.cursor()
            await cur.execute(sql.replace('?', '%s'), args)
            # 获取执行的影响数据数目
            affected = cur.rowcount
            await cur.close()
        except BaseException as e:
            raise
        # 返回结果数
        return affected


# 创建变量字符串
def create_args_string(num):
    L = []
    for n in range(num):
        L.append('?')
    return ','.join(L)


# 构建 Metaclass 的 __new__() 将Model基类的子类如User的映射信息读取处理
class ModelMetaclass(type):
    def __new__(cls, name, bases, attrs):
        # 排除掉Model类本身
        if name == "Model":
            # 直接调用父类 __new__()
            return type.__new__(cls, name, bases, attrs)

        # 获取table名称
        table_name = attrs.get('__table__', None) or name
        logging.info(f'Found model: {name} (table: {table_name})')
        # 获取所有的 Field 和 主键名
        mappings = dict()
        fields = []
        primary_key = None
        for k, v in attrs.items():
            # 判断 v 是否是 Field 的子类
            if isinstance(v, Field):
                logging.info(f'Found mapping: {k} ==> {v}')
                # 添加映射
                mappings[k] = v
                if v.primary_key:
                    if primary_key:
                        # 已经有主键
                        raise RuntimeError(f'Duplication primary key for field: {k}')
                    # 设置主键
                    primary_key = k
                else:
                    # 直接添加主键字段
                    fields.append(k)

        # 未设置primary_key
        if not primary_key:
            raise RuntimeError('Primary key not found.')
        # 将属性键名删除
        for k in mappings.keys():
            attrs.pop(k)
        # Todo: ???
        # 将剩余的字段
        escaped_fields = list(map(lambda f: '`%s`' % f, fields))

        # 保存属性和列的映射关系
        attrs['__mappings__'] = mappings
        attrs['__table__'] = table_name
        attrs['__primary_key__'] = primary_key  # 主键属性名
        attrs['__fields__'] = fields  # 主键属性外的属性名

        # 构造默认的 select, insert, update & delete sentence.
        # Todo: MySQL sentence review.
        # select id, name, age from users
        attrs['__select__'] = 'select `%s`, %s from `%s`' % (primary_key, ','.join(escaped_fields), table_name)
        # insert into users (id, name, age, id) values
        attrs['__insert__'] = 'insert into `%s` (%s, `%s`) values (%s)' % (table_name, ','.join(escaped_fields), primary_key, create_args_string(len(escaped_fields) + 1))
        # update users set
        attrs['__update__'] = 'update `%s` set %s where `%s`=?' % (table_name, ','.join(map(lambda f: '`%s`=?' % (mappings.get(f).name or f), fields)), primary_key)
        # delete from users where id = ?
        attrs['__delete__'] = 'delete from `%s` where `%s`=?' % (table_name, primary_key)

        return type.__new__(cls, name, bases, attrs)


# 从dict中继承, 可以操作：user['id']
# 实现 __getattr__() & __setattr__(), 可以操作：user.id
class Model(dict, metaclass=ModelMetaclass):

    def __init__(self, **kw):
        super(Model, self).__init__(**kw)

    def __getattr__(self, key):
        try:
            return self[key]
        except KeyError:
            raise AttributeError(r"'Model' object has no attribute '%s'" % key)

    def __setattr__(self, key, value):
        self[key] = value

    def getValue(self, key):
        return getattr(self, key, None)

    def getValueOrDefault(self, key):
        value = getattr(self, key, None)
        if value is None:
            field = self.__mappings__[key]
            if field.default is not None:
                value = field.default() if callable(field.default) else field.default
                logging.debug(f'using default value for {key} : {str(value)}')
                setattr(self, key, value)

        return value

    @classmethod
    async def findAll(cls, where=None, args=None, **kw):
        # Find objects by where clause
        sql = [cls.__select__]
        if where:
            sql.append('where')
            sql.append(where)
        if args is None:
            args = []
        orderBy = kw.get('orderBy', None)
        if orderBy:
            sql.append('order by')
            sql.append(orderBy)
        limit = kw.get('limit', None)
        if limit is not None:
            sql.append('limit')
            if isinstance(limit, int):
                sql.append('?')
                args.append(limit)
            elif isinstance(limit, tuple) and len(limit) == 2:
                sql.append('?, ?')
                args.extend(limit)
            else:
                raise ValueError(f'Invalid limit value: {str(limit)}')

        rs = await select(' '.join(sql), args)
        return [cls(**r) for r in rs]

    @classmethod
    async def findNumber(cls, selectField, where=None, args=None):
        # find number by select and where
        sql = ['select % _num_ from `%s`' % (selectField, cls.__table__)]
        if where:
            sql.append('where')
            sql.append(where)
        rs = await select(' '.join(sql), args, 1)
        if len(rs) == 0:
            return None
        return rs[0]['_num_']

    @classmethod
    async def find(cls, pk):
        'find object by primary key.'
        rs = await select('%s where `%s`=?' % (cls.__select__, cls.__primary_key__), [pk], 1)
        if len(rs) == 0:
            return None
        return cls(**rs[0])

    async def save(self):
        args = list(map(self.getValueOrDefault, self.__field__))
        args.append(self.getValueOrDefault(self.__primary_key__))
        rows = await execute(self.__insert__, args)
        if rows != 1:
            logging.warn(f'Failed to insert record: affected rows：{rows}')

    async def update(self):
        args = list(map(self.getValue, self.__fields__))
        args.append(self.getValue(self.__primary_key__))
        rows = await execute(self.__update__, args)
        if rows != 1:
            logging.warn(f'Failed to update by primary key: affected row: {rows}')

    async def remove(self):
        args = [self.getValue(self.__primary_key__)]
        rows = await execute(self.__delete__, args)
        if rows != 1:
            logging.warn(f'Failed to remove by primary key: affected rows: {rows}')


class Field(object):
    def __init__(self, name, column_type, primary_key, default):
        self.name = name
        self.column_type = column_type
        self.primary_key = primary_key
        self.default = default

    def __str__(self):
        return '<%s, %s:%s>' % (self.__class__.__name__, self.column_type, self.name)


class StringField(Field):
    def __init__(self, name=None, primary_key=False, default=None, ddl='varchar(100'):
        super().__init__(name, ddl, primary_key, default)


class BooleanField(Field):
    def __init__(self, name=None, default=0):
        super().__init__(name, 'boolean', False, default)


class IntegerField(Field):
    def __init__(self, name=None, primary_key=False, default=0):
        super().__init__(name, 'bigint', primary_key, default)


class FloatField(Field):
    def __init__(self, name=None, primary_key=False, default=0.0):
        super().__init__(name, 'real', primary_key, default)


class TextField(Field):
    def __init__(self, name=None, default=None):
        super().__init__(name, 'text', False, default)
