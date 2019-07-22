from setuptools import setup, find_packages

setup(name='infrabbitmq',
      version='0.0.1',
      author='Bifer Team',
      description='felix rabbitmq infrastructure components',
      platforms='Linux',
      packages=find_packages(exclude=['ez_setup',
                                      'examples',
                                      'tests',
                                      'specs'
                                      'integration_tests']),
      install_requires=['puka==0.0.6',
                        'infcommon'],
      dependency_links=['git+https://github.com/aleasoluciones/infcommon.git#egg=infcommon'],
      scripts=['bin/event_processor.py',
               'bin/event_publisher.py',
               'bin/queue_delete.py',
               'bin/queue_unbind.py']
      )
