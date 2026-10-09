/**
 * Sensitive-name policy for the zero-config presence and cursor projections.
 *
 * This is deliberately separate from stripInternal(): that public helper is a
 * credential/log redactor, while a broadcast default also has to remove common
 * personal data and request transport metadata. An explicit select callback is
 * the opt-in escape hatch for applications that intentionally share a field.
 */

import { stripInternal } from './sensitive.js';
import { isUnsafeProjectionFieldName } from './sensitive-names.js';

export { isUnsafeProjectionFieldName };

/** Apply the broadcast policy through stripInternal's bounded graph walk. */
export function projectDefaultUserData(value) {
	return stripInternal(value, undefined, undefined, undefined, isUnsafeProjectionFieldName);
}
