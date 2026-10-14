import { defineEnvVars } from '@sveltejs/kit/env';

const string = { schema: (value) => value };
export const variables = defineEnvVars({
	KIT3_PRIVATE_VALUE: string,
	REDIS_URL: string,
	DATABASE_URL: string,
	TEST_NAMESPACE: string
});
