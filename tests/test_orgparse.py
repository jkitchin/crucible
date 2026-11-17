"""Tests for org-mode article parsing."""

import pytest

from crucible.browse import org_to_html
from crucible.orgparse import parse_org, parse_org_file


def test_org_keywords_are_case_insensitive():
    text = "#+title: Lowercase title\n#+filetags: :concept:kinetics:\n"

    meta = parse_org(text)
    title, _, filetags = org_to_html(text)

    assert meta.title == "Lowercase title"
    assert meta.filetags == ["concept", "kinetics"]
    assert title == "Lowercase title"
    assert filetags == ["concept", "kinetics"]


def test_empty_keywords_do_not_take_the_next_line():
    text = "#+title:\n#+filetags: :concept:\n* Heading\n"
    meta = parse_org(text)
    title, _, filetags = org_to_html(text)
    assert meta.title == title == ""
    assert meta.filetags == filetags == ["concept"]

    text = "#+title: Article\n#+filetags:\n* Heading\n"
    meta = parse_org(text)
    title, _, filetags = org_to_html(text)
    assert meta.title == title == "Article"
    assert meta.filetags == filetags == []


def test_empty_property_does_not_take_the_next_line(tmp_path):
    f = tmp_path / "a.org"
    f.write_text(
        "#+TITLE: A\n\n* A\n:PROPERTIES:\n:ARTICLE_TYPE: summary\n"
        ":SOURCE_KEYS:\n:DERIVED_FROM: ../concepts/b.org\n:STATUS: draft\n:END:\n"
    )
    props = parse_org_file(f).properties
    assert props["SOURCE_KEYS"] == ""
    assert props["DERIVED_FROM"] == "../concepts/b.org"
    assert props["STATUS"] == "draft"


def test_property_drawer_is_case_insensitive():
    text = (
        "#+title: A\n\n* A\n:properties:\n:article_type: summary\n"
        ":Source_Keys: smith2024\n:end:\n"
    )
    meta = parse_org(text)
    assert meta.properties["ARTICLE_TYPE"] == "summary"
    assert meta.properties["SOURCE_KEYS"] == "smith2024"
    _, body, _ = org_to_html(text)
    assert "smith2024" in body
    assert ":properties:" not in body


@pytest.mark.parametrize(
    "text, keys",
    [
        # org-ref v2
        ("cite:smith2024", ["smith2024"]),
        ("citep:smith2024,doe2020", ["smith2024", "doe2020"]),
        # org-ref v3, bare and as links with prefix/suffix notes
        ("cite:&smith2024", ["smith2024"]),
        ("see citep:&a;&b.", ["a", "b"]),
        ("[[cite:&smith2024]]", ["smith2024"]),
        ("[[citep:See;&a pp. 3;&b; and more]]", ["a", "b"]),
        ("[[cite:&x][the paper]]", ["x"]),
        # other org-ref link types
        ("citeauthor:&x and Citet:&y", ["x", "y"]),
        # BibTeX keys may contain '.' and ':'; trailing punctuation is not a key
        ("cite:doe.2020.", ["doe.2020"]),
        ("cite:doe:2020, then", ["doe:2020"]),
        ("citet*:&a and citeyearpar:&b", ["a", "b"]),
        # separators: a space before a v3 key, or ';' between v2 keys
        ("cite:&a, &b", ["a", "b"]),
        ("cite:a;b", ["a", "b"]),
        # words that are not org-ref link types are not citations
        ("excite:foo", []),
        ("cites:many", []),
        # nor is "cite:" inside a URL or verbatim text
        ("[[https://example.com/cite:foo][a link]]", []),
        ("[[https://example.com/cite:foo]]", []),
        ("https://example.com/?q=cite:foo", []),
        ("=cite:x=", []),
    ],
)
def test_citation_keys(text, keys):
    assert parse_org(text).cite_keys == keys


def test_v3_citations_render_as_citations():
    _, body, _ = org_to_html("* A\nSee [[cite:&smith2024][Smith]] and citep:&a;&b.\n")
    for key in ("smith2024", "a", "b"):
        assert f'href="/_references#{key}"' in body
    assert "&amp;" not in body


def test_urls_containing_cite_render_as_links():
    _, body, _ = org_to_html("* A\nSee [[https://example.com/cite:foo][a link]].\n")
    assert '<a href="https://example.com/cite:foo" target="_blank">a link</a>' in body
    assert "\x01" not in body


@pytest.mark.parametrize(
    "text",
    [
        "#+begin_src emacs-lisp\n(insert \"cite:&inside [[file:a.org]]\")\n#+end_src\n",
        "#+BEGIN_SRC python\ncite:&inside [[file:a.org]]\n#+END_SRC\n",
        "  #+begin_example\n  cite:&inside [[file:a.org]]\n  #+end_example\n",
        "#+begin_export html\ncite:&inside [[file:a.org]]\n#+end_export\n",
        "Inline code ~cite:&inside~ is literal too.\n",
    ],
)
def test_literal_blocks_hold_no_citations_or_links(text):
    meta = parse_org("* A\n" + text + "Prose cites cite:&outside, see [[file:b.org]].\n")
    assert meta.cite_keys == ["outside"]
    assert meta.file_links == ["b.org"]


@pytest.mark.parametrize("begin, end", [("#+begin_src", "#+end_src"),
                                        ("  #+BEGIN_SRC", "  #+END_SRC")])
def test_src_blocks_render_as_code_in_any_case(begin, end):
    _, body, _ = org_to_html(f"* A\n{begin}\nfirst line cite:&k /x/\n{end}\n")
    assert "<pre><code>" in body  # no language: the first line is code
    assert "first line cite:&amp;k /x/" in body
    assert "citation" not in body and "<em>" not in body


@pytest.mark.parametrize(
    "text, html",
    [
        ("see https://x.org/a/?q=b now",
         'see <a href="https://x.org/a/?q=b" target="_blank">https://x.org/a/?q=b</a> now'),
        ("end https://x.org/path/.",
         'end <a href="https://x.org/path/" target="_blank">https://x.org/path/</a>.'),
        ("(https://x.org/a/b), ok",
         '(<a href="https://x.org/a/b" target="_blank">https://x.org/a/b</a>), ok'),
        ("(see https://w.org/wiki/F_(b))",
         '(see <a href="https://w.org/wiki/F_(b)" target="_blank">https://w.org/wiki/F_(b)</a>)'),
        ("[[https://x.org/a/b]] ok",
         '<a href="https://x.org/a/b" target="_blank">https://x.org/a/b</a> ok'),
    ],
)
def test_bare_urls_render_as_links(text, html):
    _, body, _ = org_to_html(f"* A\n{text}\n")
    assert html in body
    assert "<em>" not in body


def test_italics_still_render_next_to_urls():
    _, body, _ = org_to_html("* A\nSee https://x.org/a/b and /this/ word.\n")
    assert "<em>this</em>" in body


def test_article_text_is_html_escaped():
    _, body, _ = org_to_html(
        "* Head <b>x</b>\n"
        "a < b & c <script>alert(1)</script> *bold <i>* $a<b$\n\n"
        "$$x > 1$$\n"
    )
    assert "<script>" not in body and "<b>" not in body and "<i>" not in body
    assert '<h2 id="head-b-x-b">Head &lt;b&gt;x&lt;/b&gt;</h2>' in body
    assert "a &lt; b &amp; c &lt;script&gt;alert(1)&lt;/script&gt;" in body
    assert "<strong>bold &lt;i&gt;</strong>" in body
    assert r"\(a&lt;b\)" in body and r"\[x &gt; 1\]" in body


@pytest.mark.parametrize(
    "text, html",
    [
        ("an /it/, (/x/) and *b*.", "an <em>it</em>, (<em>x</em>) and <strong>b</strong>."),
        ('"*q*"', '"<strong>q</strong>"'),
        # slashes inside words or tags are not emphasis markers
        ("and/or 3/4 and 1/2.", "and/or 3/4 and 1/2."),
        # a '/' after ')' does not open emphasis (it did, swallowing "/b c /d")
        ("(a)/b c /d/", "(a)/b c <em>d</em>"),
    ],
)
def test_emphasis_follows_org_boundaries(text, html):
    _, body, _ = org_to_html(f"* A\n{text}\n")
    assert html in body
