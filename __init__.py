#-*- coding: utf-8 -*-

"""Threadsafe database wrapper library over psycopg

Very simple---- Oops, not so simple. File descriptors are copied on forks, so
the thread-safe variant must take pid into account.

>>> Q = Query("SELECT * FROM pg_catalog.pg_class WHERE relname = %s",
...           [('relname', str)], {'relname': 'pg_class'})
>>> cls = Q()
>>> len(cls) > 0
True

@author:       Anders Eurenius <anders.eurenius@favoptic.com>
@author:       Ulf Renman <ulf.renman@favoptic.com>
@organization: Favoptic Glasögondirekt AB
@copyright:    (c) 2007 Favoptic Glasögondirekt AB
@copyright:    (c) 2007- Anders Eurenius <aes@nerdshack.com>
@license:      Free Software Foundation GNU Public Licence v2
"""

import sys, os, time, thread, logging

log = logging.getLogger('pgdb2')

import psycopg2        as pg
import psycopg2.extras as ex
import psycopg2.pool   as pool

thr_lvl = ['nothing.',
           'module, but not conns or cursors.',
           'module and conns, but not cursors.',
           'module, conns and cursors.']

iso_lvl = ['Autocommit','Read committed','Read uncommitted',
           'Repeatable read', 'Serializable']

ver_pts = {'dec': 'the Decimal type',
           'mx':  'mx.DateTime',
           'dt':  'Python built-in datetime',
           'ext': 'psycopg extensions ',
           'pq3': 'pg protocol v3',}

def interpret_version(pg):
    v = pg.__version__
    v = v[v.index('(')+1 : v.index(')')]
    l = []
    for s in v.split():
        if s in ver_pts: l.append(ver_pts[s])
        else:            l.append('Unknown feature:%r' % s)
    return l

def log_version_caps(pg, lvl=logging.INFO):
    """Emit underlying psycopg2 version data into logs.

    This is called when the module is loaded, but left in
    """
    log.log(lvl, 'psycopg2:      %s' % pg.__version__)
    log.log(lvl, 'support compiled for:')
    for z in interpret_version(pg):
        log.log(lvl, '  '+z)
    log.log(lvl, 'API level:    %s' % pg.apilevel)
    log.log(lvl, 'Param style:  %s' % pg.paramstyle)
    log.log(lvl, 'Threads share %s' % thr_lvl[pg.threadsafety])

log_version_caps(pg)

# some common utils:
def nop(x):
    """Identity transform"""
    return x

def null(t):
    """Creates a converter to type-or-null"""
    def f(x):
        try:    return t(x)
        except: None
    return f

class DSQuery( object ):
    """Utility for reusing a query in a safe and convenient way.
    
    The instance is created with
      1. the query string,
      2. an ordered list of pairs of parameters and their casting (or
         conversion, or..) functions. (optional)
      3. a dictionary containing default values.
    
    When called the argument defaults to an empty dictionary. If a dict is
    given on the other hand, the defaults are copied, the copy is then updated
    with the argument dict. A list of query paramenters is then constructed by
    picking the dict items according to the keys list and mapping them with
    their corresponding functions.
    
    The same dict key can be used more than once in the query.
    
    If the query fails because of a db restart, it tries to reconnect.
    
    Although the class is crafted for that use,
      1. The query need not be a select,
      2. The keys need not be strings and
      3. The functions need not be constructors; notably, they can return None
    """
    def __init__(self, pool, sql, keys=(), defaults={}, autocommit=True):
        # There should be a nice trick for this, like __dict__ = dict(locals())
        self.sql, self.keys, self.defaults = sql, keys, defaults
        self.pool, self.autocommit = pool, autocommit
    def __repr__(self):
        return ('%s("""%s""" x (%s) x {%s})' %
                (self.__class__.__name__, self.sql,
                 ', '.join([ k for k, f in self.keys]),
                 ', '.join(  self.defaults.keys()    ) ))
    def prep_list(self, d):
        e = dict(self.defaults)                           # copy the defaults
        try:
            if isinstance( d, dict ):
                e.update(d)                               # override defaults
                l = [ f(e.get(k)) for k, f in self.keys ] # unroll args
            else:                                         # assume obj instead
                l = [ f( getattr(d, k, e.get(k)) ) for k, f in self.keys ]
            return l
        except Exception, x:
            log.error('Exception occured when preparing arguments.')
            log.exception(x)
            raise
    def _handle_exception(self, con, x, z=0):
        try:
            if con:
                con.rollback()
                self.pool.putconn(con)
        except: pass
        log.error('Exception occured when executing query.')
        log.exception(x)
        raise x
    def __call__(self, d={}, retry=1):
        prep = self.prep_list(d)
        if log.isEnabledFor(logging.DEBUG):
            log.debug('pre-prep:  %r' % self.sql)
        con = None
        try:
            con = self.pool.getconn()
            cur = con.cursor()
            cur.execute(self.sql, prep)
            if log.isEnabledFor(logging.DEBUG):
                log.debug('query:     %r' % cur.query)
                log.debug('status:    %r' % cur.statusmessage)
                log.debug('row count: %d' % cur.rowcount)
            if cur.description: ret = cur.fetchall()
            else:               ret = cur.rowcount
            if self.autocommit: con.commit()
            self.pool.putconn(con)
            return ret
        except pg.OperationalError, x:
            log.warning(x)
            if retry:
                con.close()
                self.pool.putconn(con)
                log.warning('con was closed, reconnecting... ')
                return self(d, retry-1)
            else:
                log.warning('con was closed, NOT reconnecting.')
                raise
        except Exception, x:
            log.exception(x)
            if con:
                con.rollback()
                self.pool.putconn(con)
            raise

class DSCompatQuery( DSQuery ):
    """Query functors compatible with old pgdb behaviour.

    The only overridden method here is __call__.
    """
    def __call__(self, d={}, retry=1):
        "Implements old behaviour, returning actual dicts and -1 on errors."
        try:    return [ dict(e) for e in DSQuery.__call__(self, d, retry) ]
        except: return -1

class DataSource( pool.ThreadedConnectionPool ):
    """PsycoPg2 ThreadedConnectionPool subclass that creates DSQuery functors.
    
    The slightly roundabout method of creating query functors has advantages:
      1. We now use the PsycoPg2 threading safeties.
      2. We can now get query functors from different data sources.
      3. We now have easier control over reconnecting to the same datasource.
    """
    def __init__(self, dsn='', maxconn=8, minconn=1):
        super(DataSource, self).__init__(minconn, maxconn, dsn=dsn,
                                         connection_factory=ex.DictConnection)
        self.dsn = dsn
    def query(self, sql, keys=(), defaults={}, autocommit=True):
        return DSQuery(self, sql, keys, defaults, autocommit)
    def compat(self, sql, keys=(), defaults={}, autocommit=True):
        return DSCompatQuery(self, sql, keys, defaults, autocommit)

module_ds = None

def reset():
    """Discards the module-global datasource for the old pgdb compatibility."""
    global module_ds
    module_ds = None

def Query(sql, keys=(), defaults={}, autocommit=True):
    """Constructs a callable compatible with the old pgdb.Query.
    
    >>> import sys, imp
    >>> sys.modules['config'] = imp.new_module('config')
    >>> q1 = Query('SELECT 1 AS x;')
    >>> reset()
    >>> import config
    >>> config.dsn = ''
    >>> q1 = Query('SELECT 1 AS x;')
    >>> reset()
    >>> sys.argv.extend(['--dsn',''])
    >>> q1 = Query('SELECT 1 AS x;')
    >>> q1()
    [{'x': 1}]
    """
    global module_ds
    if not module_ds:
        if ('--dsn' in sys.argv and
            0 < sys.argv.index('--dsn') + 1 < len(sys.argv)):
            dsn = sys.argv[sys.argv.index('--dsn')+1]
            log.info('Took DSN from cmd line.')
        else:
            try:
                import config            # import global config if there is one
                dsn = config.dsn
                log.info('Took DSN from config module.')
            except:
                dsn =  ''
                log.warn('Using empty DSN.')
        module_ds = DataSource(dsn = dsn)
    return module_ds.compat(sql, keys, defaults)

class CachedQuery( list ):
    """Result caching list class that refreshes itself.
    
    The instance is given
      1. a function or functor, (L{Query}, hint, hint.)
      2. an optional cache time in seconds
      3. a function the query results are mapped through
    
    The point of the exercise is to get stuff from the db while balancing
      1. not doing a query every time
      2. allowing change without restarting the application
    
    In case of failure, the 'list' is empty. While this is not a good idea,
    it still is a less bad default.
    
    It can be refreshed manually with C{refresh}, so if you like, you can set
    the cache time to 2**64 and refresh it explicitly.
    
    >>> cq = CachedQuery(Query("SELECT relname FROM pg_catalog.pg_class"),
    ...                  f=lambda x: (x['relname'].capitalize()))
    >>> len(cq) > 0
    True
    
    @warning: Failure semantics are not so well thought out.
    """
    def __init__(self, q, to=300, f=nop):
        self.q, self.to, self.f, self.t = q, to, f, 0
        self.refresh()
    def refresh(self):
        log.info('REFRESH: '+str(self.q))
        try:
            self.t, l = time.time(), map(self.f, self.q())
        except Exception, x:
            log.exception(x)
            l = []
        self[:] = l
        log.debug('REFRESH: len: %d', len(l))
    def check(self):
        log.debug('CHECK: '+str(self.q))
        if time.time() - self.t > self.to:
            self.refresh()
    def __getitem__(self, *x): self.refresh();return list.__getitem__(self,*x)
    def __getslice__(self,*x): self.refresh();return list.__getslice__(self,*x)
    def __repr__(self,*x):     self.refresh();return list.__repr__(self, *x)
    def __str__(self,*x):      self.refresh();return list.__str__(self, *x)
    def __len__(self,*x):      self.refresh();return list.__len__(self, *x)

if __name__ == '__main__':
    import sys, logging, doctest
    logging.basicConfig(stream=sys.stdout, format='%(levelname)s %(message)s')
    doctest.testmod()
