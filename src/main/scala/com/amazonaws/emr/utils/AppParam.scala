package com.amazonaws.emr.utils

/**
 * Represents a single application parameter used in CLI argument parsing.
 *
 * Each `AppParam` defines a command-line argument's name, an optional description,
 * and whether it is optional. This abstraction helps in generating usage strings,
 * examples, and templates for documentation or validation purposes.
 *
 * Example:
 * {{{
 *   AppParam("filename", "Path to the Spark event log", isOptional = false)
 * }}}
 *
 * Would render as:
 *   - Option: `--filename`
 *   - Template: `<FILENAME>`
 *   - Example: `  FILENAME                Path to the Spark event log`
 *
 * @param name        The name of the parameter (used in CLI as --name).
 * @param description Human-readable description of the parameter.
 * @param isOptional  Whether the parameter is optional (default: true).
 */
case class AppParam(name: String, description: String = "", isOptional: Boolean = true) {

  /**
   * Returns a formatted example line with the uppercased parameter name
   * aligned to a fixed width, followed by its description.
   */
  def example: String = f"  ${format(name.toUpperCase)} $description"

  /** Returns the CLI flag representation of the parameter (e.g., --filename). */
  def option: String = s"--$name"

  /**
   * Returns a usage template representation, showing whether the param
   * is required (`<NAME>`) or optional (`[--name NAME]`).
   */
  def template: String =
    if (isOptional) s"[$option ${name.toUpperCase}]"
    else s"<${name.toUpperCase}>"

  /**
   * Pads the text to a fixed width for alignment in help/usage output.
   *
   * @param text Text to format.
   * @return Left-padded string with a fixed width.
   */
  private def format(text: String): String = {
    val fixedWidth = 25
    text.padTo(fixedWidth, ' ')
  }

}