// Small project policy: shared tokens, flat selectors, and no accidental overrides.
export default {
  rules: {
    "at-rule-no-unknown": true,
    "block-no-empty": true,
    "color-no-invalid-hex": true,
    "declaration-block-no-duplicate-properties": true,
    "declaration-no-important": true,
    "no-duplicate-selectors": true,
    "selector-max-id": 0,
    "selector-max-specificity": "0,3,4",
    "declaration-property-value-disallowed-list": {
      "font-size": ["/^(?!var\\(--text-).+/"],
      "font-family": ["/^(?!var\\(--font-).+/"],
      "/color$/": ["/#/"],
      background: ["/#/"],
      border: ["/#/"],
      "box-shadow": ["/#/"],
    },
  },
  overrides: [
    {
      files: ["src/styles/tokens.css"],
      rules: { "declaration-property-value-disallowed-list": null },
    },
  ],
};
