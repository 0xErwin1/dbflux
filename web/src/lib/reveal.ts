/**
 * Scroll reveal for the site pages.
 *
 * `.reveal` marks one element and `.reveal-group` marks a container whose
 * direct children reveal one after another. The hidden state only applies
 * while `<html>` carries `js-reveal`, which the layout's head script sets
 * before first paint when motion is allowed and IntersectionObserver exists.
 * Without that class every element renders in place.
 */

export const REVEAL_CLASS = 'js-reveal';
export const REVEAL_TARGETS = '.reveal, .reveal-group > *';

const REVEAL_ANIMATION = 'reveal-in';

/** Delay for the nth element entering in the same batch, capped so long lists stay short. */
export function staggerDelay(index: number, stepMs: number, maxMs: number): number {
  if (!Number.isFinite(index) || index <= 0) return 0;
  if (!Number.isFinite(stepMs) || stepMs <= 0) return 0;

  const cap = Number.isFinite(maxMs) && maxMs > 0 ? maxMs : 0;
  return Math.min(Math.round(index * stepMs), cap);
}

/** Parses a CSS time (`60ms`, `0.4s`) into milliseconds, or returns the fallback. */
export function parseTimeMs(value: string | null | undefined, fallback: number): number {
  const match = /^\s*(-?\d*\.?\d+)\s*(ms|s)\s*$/i.exec(value ?? '');
  if (!match) return fallback;

  const amount = Number(match[1]) * (match[2].toLowerCase() === 's' ? 1000 : 1);
  return Number.isFinite(amount) && amount >= 0 ? amount : fallback;
}

/**
 * Whether an element that is not intersecting has already been scrolled past.
 * Such elements are shown without animation so scrolling back up never finds a gap.
 */
export function isAboveViewport(rectBottom: number): boolean {
  return rectBottom <= 0;
}

function documentOrder(left: Element, right: Element): number {
  if (left === right) return 0;
  return left.compareDocumentPosition(right) & Node.DOCUMENT_POSITION_FOLLOWING ? -1 : 1;
}

/**
 * Observes every reveal target on the page and animates each one once, the
 * first time it enters the viewport. Does nothing unless the head script
 * enabled reveals, so reduced motion and missing IntersectionObserver keep all
 * content visible.
 */
export function initScrollReveal(root: HTMLElement = document.documentElement): void {
  if (!root.classList.contains(REVEAL_CLASS)) return;
  root.dataset.revealReady = '';

  const style = getComputedStyle(root);
  const stepMs = parseTimeMs(style.getPropertyValue('--reveal-stagger'), 60);
  const maxMs = parseTimeMs(style.getPropertyValue('--reveal-stagger-max'), 360);

  const finish = (element: HTMLElement) => {
    element.dataset.revealed = 'done';
    element.style.removeProperty('--reveal-delay');
  };

  const observer = new IntersectionObserver(
    (entries) => {
      const entering: HTMLElement[] = [];

      for (const entry of entries) {
        const element = entry.target as HTMLElement;

        if (entry.isIntersecting) {
          entering.push(element);
        } else if (isAboveViewport(entry.boundingClientRect.bottom)) {
          observer.unobserve(element);
          finish(element);
        }
      }

      entering.sort(documentOrder);

      entering.forEach((element, index) => {
        observer.unobserve(element);
        element.style.setProperty('--reveal-delay', `${staggerDelay(index, stepMs, maxMs)}ms`);
        element.dataset.revealed = 'run';
      });
    },
    { rootMargin: '0px 0px -8% 0px' },
  );

  const onAnimationDone = (event: AnimationEvent) => {
    const element = event.currentTarget as HTMLElement;
    if (event.target !== element || event.animationName !== REVEAL_ANIMATION) return;

    element.removeEventListener('animationend', onAnimationDone);
    element.removeEventListener('animationcancel', onAnimationDone);
    finish(element);
  };

  for (const element of document.querySelectorAll<HTMLElement>(REVEAL_TARGETS)) {
    if (element.dataset.revealed) continue;

    // Content not rendered at load (an inactive tab, a phone-only row) shows
    // instantly later, so switching a demo never replays an entrance.
    if (element.getClientRects().length === 0) {
      finish(element);
      continue;
    }

    element.addEventListener('animationend', onAnimationDone);
    element.addEventListener('animationcancel', onAnimationDone);
    observer.observe(element);
  }
}
