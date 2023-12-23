from .language import *
from .dataset import Dataset
from .runtime import Runtime
from .sandbox import Query

import sys

ARGS = set(sys.argv[1:])


def ASSERT_EQ(a, b):
    if not a == b:
        raise RuntimeError(f"Assertion failed: {a} != {b}")


def ASSERT_TOKEN_LISTS(t1, t2, s, log):
    if t1 != t2:
        print(
            f"Assertion failed for tokenized result of '{s}':"
            f"\ntoken result   {t1}"
            f"\n               !="
            f"\nassumed tokens {t2}"
        )
        print("Log:")
        print(log)
        raise RuntimeError("Assertion failure during test (see above).")


def ASSERT_TOKENS(s, toks):
    # DOES NOT ASSERT FOR ANY ASSOCIATED METADATA.
    tk = Tokenizer()
    tokens = tk.tokenize(s)
    ASSERT_TOKEN_LISTS(tokens, toks, s, tk.dump_log())


def ASSERT_TOKENS_AND_DATA(s, toks):
    # ASSERTS FOR ANY ASSOCIATED METADATA.
    tk = Tokenizer()
    tokens = tk.tokenize(s)
    ASSERT_TOKEN_LISTS(tokens, toks, s, tk.dump_log())
    for i, (t1, t2) in enumerate(zip(tokens, toks)):
        if t1.data != t2.data:
            print(f"Assertion failed on token {i}: {t1} != {t2}")


def ASSERT_THROW(f, *args, **kwargs):
    astr = f"{args}"
    kstr = f"{kwargs}"
    try:
        f(*args, **kwargs)
    except:
        return
    raise RuntimeError(f"Assertion failed: {f.__name__} did not throw with arguments {astr}, {kstr}")


def ASSERT_THROW_WITH_LOG(o: Component, f, *args, **kwargs):
    astr = f"{args}"
    kstr = f"{kwargs}"
    try:
        f(*args, **kwargs)
    except:
        return
    print("With log:")
    print(o.dump_log())
    raise RuntimeError(f"Assertion failed: {f.__name__} did not throw with arguments {astr}, {kstr}")


test_registry = list()


def Test(f):
    def _test():
        f()
        print(f"All assertions passed ({f.__name__})")

    test_registry.append(_test)
    return _test


@Test
def test_tokenizer():
    ASSERT_TOKENS("", list())
    ASSERT_TOKENS("|", [PIPE()])
    ASSERT_TOKENS("| hello world", [PIPE(), STRING(), STRING()])

    ASSERT_TOKENS("hello function()", [STRING(), FUNCTION()])
    ASSERT_TOKENS(
        "hello function() | goodbye function() another()", [STRING(), FUNCTION(), PIPE(), STRING(), FUNCTION(), FUNCTION()]
    )

    ASSERT_TOKENS_AND_DATA("pred 'some argument' | other", [STRING("pred"), STRING("some argument"), PIPE(), STRING("other")])

    ASSERT_TOKENS("+test debug |", [Tokens.PARAMETERS, Tokens.PIPE])


@Test
def test_compiler():
    ASSERT_EQ(not Expression(0), True)
    cmp = Compiler()
    tk = Tokenizer()

    def chained(s):
        return cmp.compile(tk.tokenize(s), source=s)[0]

    # Basic compiler checks - we can't have empty expressions.
    ASSERT_THROW_WITH_LOG(cmp, cmp.compile, [PIPE(c=0), PIPE(c=1)])

    # Test basic compilation.
    ASSERT_EQ(cmp.compile([PIPE()])[0], [Expression(None)])
    ASSERT_EQ(chained("| pred"), [Expression(None, "pred")])

    # Another check - no leading functions, for now.
    ASSERT_THROW_WITH_LOG(cmp, cmp.compile, [PIPE(c=0), FUNCTION("", "")])

    # Test a bunch of random expressions and pipelines.
    ASSERT_EQ(chained("| pred func(abc, def)"), [Expression(None, "pred", [("func", "abc, def")])])
    ASSERT_EQ(chained("| pred func(abc, def) arg"), [Expression(None, "pred", [("func", "abc, def"), "arg"])])
    ASSERT_EQ(
        chained("| pred func(abc) | pred2 arg"),
        [Expression(None, "pred", [("func", "abc")]), Expression(None, "pred2", ["arg"])],
    )

    # Test that we can handle parameters. Make a custom compiler for these, as they might modify state.
    ASSERT_EQ(Compiler().compile(tk.tokenize("+test | pred func(abc)"))[0], [Expression(None, "pred", [("func", "abc")])])


@Test
def test_runtime():
    cmp = Compiler()
    tk = Tokenizer()
    datasets = {
        "default": Dataset("Default", ["str1", "str2"]),
        "all": Dataset("All", ["str3", "str4"]),
    }
    rt = Runtime(datasets=datasets, commands=dict())

    def chained(s):
        return cmp.compile(tk.tokenize(s), source=s)

    # It's so beautiful.
    prog, params = chained("+test | commands | info | index all | info")
    ASSERT_EQ(rt.execute(prog, params).l, ["str3", "str4"])

    # kinda silly test but ok
    ASSERT_EQ(rt.execute(*chained("+test")).name, "Default")


@Test
def test_timing():
    DO_TIMING = "timing" in ARGS

    def q(s):
        if not DO_TIMING:
            return s
        if s.startswith("+"):
            return f'+timing debug {s.lstrip("+")}'
        return f'+timing debug | {s.lstrip("|")}'

    datasets = {
        "default": Dataset("Default", ["str1", "str2"]),
        "all": Dataset("All", ["str3", "str4"]),
    }
    rt = Runtime(datasets=datasets, commands=dict())

    def chained(s):
        return Compiler().compile(Tokenizer().tokenize(s), source=s)

    ASSERT_EQ(chained(q("|"))[0], [Expression(None, None)])

    # Keep this one disabled in general, lol.
    ASSERT_EQ(rt.execute(*chained(q("+test | wait 2.1 | commands"))), datasets["default"])


@Test
def test_query():
    # Test full queries, with actual data.
    QUERYDEBUG = False

    def q(s):
        if not QUERYDEBUG:
            return s
        if s.startswith("+"):
            return f'+timing debug {s.lstrip("+")}'
        return f'+timing debug |{s.lstrip("|")}'

    # Prerequisites:
    import os

    ASSERT_EQ(os.path.isdir("klunk/samples"), True)

    # Run a bunch of random queries to make sure we don't crash
    # This is essentially regression testing, as these were all
    # just "write this with debug on to test if it runs" and now
    # we don't want the debug output so, meh. We could test more
    # (like make sure the values are correct on return) but that
    # is way too much effort for pretty low gain.. meh, later..?
    Query(q("+test | info")).run()
    Query(q("+test | index all | info")).run()
    Query(q("+test | testlog 'Hello world!'")).run()

    # Run more random queries. None of these should throw.
    Query(q("+test | players")).run()
    Query(q("+test | players | extract uuid")).run()
    ASSERT_EQ(len(Query(q("+test | players | take last 5 | count")).run().l), 5)
    ASSERT_EQ(len(Query(q("+test | players | take 5 | count")).run().l), 5)
    Query(q("+test | players | vars | attrs | metainfo | help | help metainfo | index all")).run()


@Test
def test_utils():
    ASSERT_EQ(split_before("", lambda c: c == "q"), ("", ""))
    ASSERT_EQ(split_before("d", lambda c: c == "q"), ("d", ""))
    ASSERT_EQ(split_before("lequin", lambda c: c == "q"), ("le", "quin"))
    ASSERT_EQ(split_before(" lequin", lambda c: c == "q"), (" le", "quin"))

    ASSERT_EQ(consume_string("this is"), ("this", " is"))
    ASSERT_EQ(consume_string("this._can'tbe_believed!(lol)"), ("this._can'tbe_believed!", "(lol)"))

    ASSERT_EQ(consume_quoted("'whoa, so cool'"), ("whoa, so cool", ""))
    ASSERT_EQ(consume_quoted("'whoa, so cool''with more test"), ("whoa, so cool", "'with more test"))
    ASSERT_THROW(consume_quoted, "'whoa, so cool")

    # ew, this was originally a really ugly bug, coincidentally caught by this test...
    ASSERT_EQ(consume_quoted("'whoa, \\' even cooler'"), ("whoa, ' even cooler", ""))


@Test
def todo_tests():
    # Current behaviour -> desired behaviour

    # TODO - Support recursive parsing within functions.
    tk = Tokenizer()
    tokens = tk.tokenize("function('(')")
    # Always
    ASSERT_EQ(len(tokens), 1)
    tok = tokens[0]
    ASSERT_EQ(tok, FUNCTION())
    ASSERT_EQ(tok.data[0], "function")
    # Current
    ASSERT_EQ(tok.data[1], "'('")
    # Desired
    # ASSERT_EQ(tok.data[1], LIST())
    # ASSERT_EQ(tok.data[1][0], STRING())


if __name__ == "__main__":
    for test_f in test_registry:
        test_f()
