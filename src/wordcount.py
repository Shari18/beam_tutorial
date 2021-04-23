from __future__ import absolute_import

import argparse
import logging
import re

from past.builtins import unicode

import apache_beam as beam
from apache_beam.io import ReadFromText
from apache_beam.io import WriteToText
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.options.pipeline_options import SetupOptions

# Format the counts into a PCollection of strings.
def format_result(word, count):
  return '%s: %d' % (word, count)

class WordExtractingDoFn(beam.DoFn):
  """Parse each line of input text into words."""
  def process(self, element):
    """Returns an iterator over the words of this element.

    The element is a line of text.  If the line is blank, note that, too.

    Args:
      element: the element being processed

    Returns:
      The processed element.
    """
    return re.findall(r'[\w\']+', element, re.UNICODE)


def run():
  # The pipeline will be run on exiting the with block.
  with beam.Pipeline(options=PipelineOptions()) as p:

    file = '../data/kinglear.txt'
    output_file = '../data/output.txt'

    # Read the text file[pattern] into a PCollection.
    lines = p | 'Read' >> ReadFromText(file)

    split_lines = lines | 'Split' >> beam.FlatMap(lambda x: re.findall(r'[A-Za-z\']+', x))

    counts = split_lines | 'PairWithOne' >> beam.Map(lambda x: (x, 1)) | beam.CombinePerKey(sum)

    output = counts | 'Format' >> beam.MapTuple(format_result)

    # Write the output using a "Write" transform that has side effects.
    # pylint: disable=expression-not-assigned
    output | 'Write' >> WriteToText(output_file)


if __name__ == '__main__':
  logging.getLogger().setLevel(logging.INFO)
  run()