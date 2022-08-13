import { existsSync, readFileSync } from "fs";
import { dirname, join } from "path";
import { createRequire } from "module";

const { platform, arch } = process;

var suffix = `${platform}-${arch}`;

switch (platform) {
	case "android":
		if (arch === "arm") {
			suffix += "-eab";
		}
		break;
	case "win32":
		suffix += "-msvc";
		break;
	case "linux":
		if (arch === "arm") {
			suffix += "-gnueabihf";
		} else {
			if (process.report.getReport().header.glibcVersionRuntime) {
				suffix += "-gnu";
			} else {
				suffix += "-musl";
			}
		}
}

const node = `package-template.${suffix}.node`;

export default createRequire(import.meta.url)(
	existsSync(
		join(dirname(decodeURI(new URL(import.meta.url).pathname)), node),
	) ? "./" + node : `@napi-rs/package-template-${suffix}`,
);
