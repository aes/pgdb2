# pgdb2

Thread-safe database wrapper library over psycopg2

Simply, this library allows instantiating functors that

 * look like functions... written in SQL,
 * can be used like functions and
 * hide some of the ugly error handling logic

As emphasis has previously been on ease-of-use, the simplest way to get
started is to try the old way:

    >>> from pgdb2 import Query, nop
    >>> q = Query("SELECT (%s)::real AS x",
    ...           [('a', str)], {'a': '42'})
    >>> q({'a': 12})
    [{'x': 12.0}]
    >>> q(a=13)
    [{'x': 13.0}]
    >>> q(14)
    [{'x': 14.0}]
    >>> q()
    [{'x': 42.0}]

It might be a little hard to see how the first claim, (that the functors look
like functions, might work,) but as SQL grows, the prefix and post-fix gunk
fades, much like decorators.

    >>> from pgdb2 import DataSource, nop
    >>> ds = DataSource('')
    >>> class Foo:
    ...   z = ds.Query("""
    ...
    ...     SELECT 31337 * %s AS z
    ...
    ...   """, (('x', nop),) )
    ...
    >>> z = Foo.z(x=1)
    >>> type(z[0])
    <class 'psycopg2.extras.DictRow'>

### Authors and Licence

Originally created by Anders Eurenius and Ulf Renman at [Favoptic](1) in '07
or '06. The code has been significantly reworked since, but the query functor
concept is essentially unchanged

(c) 2007 Favoptic GlasÅˆgondirekt AB

(c) 2007- Anders Eurenius <aes@nerdshack.com>

Free Software Foundation GNU Public Licence v2

[1]: http://www.favoptic.com/
