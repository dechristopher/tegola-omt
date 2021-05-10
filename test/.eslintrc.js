/*
 * "off" or 0 - turn the rule off
 * "warn" or 1 - turn the rule on as a warning (doesn't affect exit code)
 * "error" or 2 - turn the rule on as an error (exit code is 1 when triggered)
 */
module.exports = {
	env: {
		browser: true,
		es6: true,
		node: false
	},
	root: true,
	parserOptions: {
		sourceType: "module"
	},
	plugins: [
		"react",
		"react-hooks",
		"eslint-plugin-jsdoc"
	],
	extends: [
		"plugin:eslint-plugin-jsdoc/recommended"
	],
	rules: {
		"require-jsdoc": [ "warn",
			{
				require: {
					ArrowFunctionExpression: true
				}
			}
		],
		"max-len": [ "warn", {
			code: 120,
			ignoreUrls: true
		} ],
		indent: [ "error", "tab" ],
		"array-bracket-spacing": [ "error", "always" ],
		"no-fallthrough": 0,
		"react-hooks/rules-of-hooks": "error",
		"react-hooks/exhaustive-deps": "warn",
		/**
		 * note you must disable the base rule as it can report incorrect errors
		 *
		 * @see https://eslint.org/docs/rules/semi
		 * @see https://github.com/typescript-eslint/typescript-eslint/blob/master/packages/eslint-plugin/docs/rules/semi.md
		 */
		/**
		 * @see https://github.com/typescript-eslint/typescript-eslint/blob/master/packages/eslint-plugin/docs/rules/no-extra-semi.md
		 * @see https://github.com/typescript-eslint/typescript-eslint/blob/master/packages/eslint-plugin/docs/rules/member-delimiter-style.md
		 */
		"quote-props": [
			"warn",
			"as-needed"
		],
		"space-before-blocks": [
			"warn",
			{
				functions: "always",
				keywords: "always",
				classes: "always"
			}
		],
		"object-curly-spacing": [
			"error", "always"
		],
		/**
		 * @see https://eslint.org/docs/rules/space-in-parens#top
		 */
		"space-in-parens": [
			"warn",
			"always"

		],
		"padded-blocks": [
			"warn",
			{
				blocks: "never",
				classes: "always",
				switches: "never"
			},
			{
				allowSingleLineBlocks: true
			}
		],
		/**
		 * @see https://eslint.org/docs/rules/no-mixed-spaces-and-tabs
		 */
		"no-mixed-spaces-and-tabs": [ "error", "smart-tabs" ],
		/**
		 * @see https://eslint.org/docs/rules/no-mixed-operators
		 */
		"no-mixed-operators": [
			"error",
			{
				allowSamePrecedence: true
			}
		]
	}
};