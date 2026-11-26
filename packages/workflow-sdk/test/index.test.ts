import { expect, it } from 'vitest';

it('loads the SDK as an ES module', async () => {
  await expect(import('../src/index.js')).resolves.toMatchObject({});
});
