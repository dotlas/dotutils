from setuptools import setup, find_packages

with open("requirements.txt") as f:
    reqs_str: str = f.read()

reqs: list[str] = reqs_str.split("\n")
reqs.remove("")
reqs = [r.split("=")[0] for r in reqs]

setup(
    name="dotutils",
    version="0.1",
    description="Package containing atomic helpers",
    url="https://github.com/dotlas/dotutils", 
    packages=find_packages(),
    python_requires=">=3.9",
    install_requires=reqs
)