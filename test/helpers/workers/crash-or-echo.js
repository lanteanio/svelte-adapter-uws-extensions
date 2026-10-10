export default function ({ input }) {
	if (input.crash) process.exit(1);
	return input.value;
}
