## Ranked Query Language

Ranked Query Language (hereafter referred to as 'RQL') is a custom pipe-expression-based query language that supports dynamic queries of the MCSR Ranked match database.

Currently, these queries can only be made either locally (using the samples provided, for testing) or through the bot, which is hosted on a server with a database clone.

### Setup

If you'd like to test out the language locally, you'll want:
- A new version of Python with support for type annotations (I use 3.10, can't guarantee earlier versions would work)
- A local virtual environment to install packages into
- To run `pip install -r requirements.txt` **after activating a virtual environment, seriously, this requirements.txt is super messy**
- To run `python -m klunk.test` to ensure tests pass locally (that is, that the setup is correct, as obviously I would never break tests... :D)
- To run `python bot.py --fake` to run the local query CLI

### Development Notes

- My autoformatter is broken for some reason :( none of these files are properly formatted
    - TODO - just install black and use that, I guess

## Language Specification

There is none.
