import setuptools

with open("README.md", "r") as fh:
    long_description = fh.read()

setuptools.setup(
    name="pikarin",
    version="0.0.1",
    author="BimaAdi",
    author_email="bimaadi419@gmail.com",
    description="High Level API for RabbitMQ.",
    long_description=long_description,
    long_description_content_type="text/markdown",
    url="https://github.com/BimaAdi/pikarin",
    packages=setuptools.find_packages(),
    install_requires=[
        "pika>=1.1.0",
        "requests>=2.23.0"
    ],
)