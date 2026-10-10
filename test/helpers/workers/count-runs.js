let count = 0;
export default async function ({ input }) {
	count++;
	if (input.delay) await new Promise(resolve => setTimeout(resolve, input.delay));
	return { count, value: input.value };
}
